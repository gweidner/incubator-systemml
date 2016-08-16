/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.instructions.spark.utils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.spark.data.SerLongWritable;
import org.apache.sysml.runtime.instructions.spark.data.SerText;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsComp.BinaryBlockToCSVFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsComp.BinaryCellToBinaryBlockFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsComp.CSVToBinaryBlockFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsComp.PrepareBinaryBlockFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsComp.SliceBinaryBlockToRowsFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsComp.TextToBinaryBlockFunction;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.CSVFileFormatProperties;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixCell;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.mapred.ReblockBuffer;
import org.apache.sysml.runtime.util.UtilFunctions;

public class RDDConverterUtils 
{
	/**
	 * 
	 * @param sc
	 * @param input
	 * @param mcOut
	 * @param outputEmptyBlocks
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> textCellToBinaryBlock(JavaSparkContext sc,
			JavaPairRDD<LongWritable, Text> input, MatrixCharacteristics mcOut, boolean outputEmptyBlocks) 
		throws DMLRuntimeException  
	{
 		//convert textcell rdd to binary block rdd (w/ partial blocks)
		JavaPairRDD<MatrixIndexes, MatrixBlock> out = input.values()
				.mapPartitionsToPair(new TextToBinaryBlockFunction(mcOut));

		//inject empty blocks (if necessary) 
		if( outputEmptyBlocks && mcOut.mightHaveEmptyBlocks() ) {
			out = out.union( 
				SparkUtils.getEmptyBlockRDD(sc, mcOut) );
		}
		
		//aggregate partial matrix blocks
		out = RDDAggregateUtils.mergeByKey( out ); 
		
		return out;
	}

	/**
	 * 
	 * @param sc
	 * @param input
	 * @param mcOut
	 * @param outputEmptyBlocks
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> binaryCellToBinaryBlock(JavaSparkContext sc,
			JavaPairRDD<MatrixIndexes, MatrixCell> input, MatrixCharacteristics mcOut, boolean outputEmptyBlocks) 
		throws DMLRuntimeException 
	{	
 		//convert binarycell rdd to binary block rdd (w/ partial blocks)
		JavaPairRDD<MatrixIndexes, MatrixBlock> out = input
				.mapPartitionsToPair(new BinaryCellToBinaryBlockFunction(mcOut));

		//inject empty blocks (if necessary) 
		if( outputEmptyBlocks && mcOut.mightHaveEmptyBlocks() ) {
			out = out.union( 
				SparkUtils.getEmptyBlockRDD(sc, mcOut) );
		}
		
		//aggregate partial matrix blocks
		out = RDDAggregateUtils.mergeByKey( out ); 
		
		return out;
	}

	/**
	 * Converter from binary block rdd to rdd of labeled points. Note that the input needs to be 
	 * reblocked to satisfy the 'clen <= bclen' constraint.
	 * 
	 * @param in
	 * @return
	 */
	public static JavaRDD<LabeledPoint> binaryBlockToLabeledPoints(JavaPairRDD<MatrixIndexes, MatrixBlock> in) 
	{
		//convert indexed binary block input to collection of labeled points
		JavaRDD<LabeledPoint> pointrdd = in
				.values()
				.flatMap(new PrepareBinaryBlockFunction());
		
		return pointrdd;
	}

	/**
	 * 
	 * @param in
	 * @param mcIn
	 * @param props
	 * @param strict
	 * @return
	 */
	public static JavaRDD<String> binaryBlockToCsv(JavaPairRDD<MatrixIndexes,MatrixBlock> in, MatrixCharacteristics mcIn, CSVFileFormatProperties props, boolean strict)
	{
		JavaPairRDD<MatrixIndexes,MatrixBlock> input = in;
		
		//fast path without, general case with shuffle
		if( mcIn.getCols()>mcIn.getColsPerBlock() ) {
			//create row partitioned matrix
			input = input
					.flatMapToPair(new SliceBinaryBlockToRowsFunction(mcIn.getRowsPerBlock()))
					.groupByKey()
					.mapToPair(new ConcatenateBlocksFunction(mcIn.getCols(), mcIn.getColsPerBlock()));	
		}
		
		//sort if required (on blocks/rows)
		if( strict ) {
			input = input.sortByKey(true);
		}
		
		//convert binary block to csv (from blocks/rows)
		JavaRDD<String> out = input
				.flatMap(new BinaryBlockToCSVFunction(props));
	
		return out;
	}
	
	/**
	 * 
	 * @param sc
	 * @param lines
	 * @param mcOut
	 * @param hasHeader
	 * @param delim
	 * @param fill
	 * @param missingValue
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> csvToBinaryBlock(JavaSparkContext sc,
			JavaPairRDD<LongWritable, Text> input, MatrixCharacteristics mcOut, 
			boolean hasHeader, String delim, boolean fill, double fillValue) 
		throws DMLRuntimeException 
	{
		//determine unknown dimensions and sparsity if required
		if( !mcOut.dimsKnown(true) ) {
			Accumulator<Double> aNnz = sc.accumulator(0L);
			JavaRDD<String> tmp = input.values()
					.map(new CSVAnalysisFunction(aNnz, delim));
			long rlen = tmp.count() - (hasHeader ? 1 : 0);
			long clen = tmp.first().split(delim).length;
			long nnz = UtilFunctions.toLong(aNnz.value());
			mcOut.set(rlen, clen, mcOut.getRowsPerBlock(), mcOut.getColsPerBlock(), nnz);
		}
		
		//prepare csv w/ row indexes (sorted by filenames)
		JavaPairRDD<Text,Long> prepinput = input.values()
				.zipWithIndex(); //zip row index
		
		//convert csv rdd to binary block rdd (w/ partial blocks)
		JavaPairRDD<MatrixIndexes, MatrixBlock> out = 
				prepinput.mapPartitionsToPair(
					new CSVToBinaryBlockFunction(mcOut, hasHeader, delim, fill, fillValue));
		
		//aggregate partial matrix blocks
		out = RDDAggregateUtils.mergeByKey( out ); 
		
		return out;
	}
	
	/**
	 * Example usage:
	 * <pre><code>
	 * import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtils
	 * import org.apache.sysml.runtime.matrix.MatrixCharacteristics
	 * import org.apache.spark.api.java.JavaSparkContext
	 * val A = sc.textFile("ranA.csv")
	 * val Amc = new MatrixCharacteristics
	 * val Abin = RDDConverterUtils.csvToBinaryBlock(new JavaSparkContext(sc), A, Amc, false, ",", false, 0)
	 * </code></pre>
	 * 
	 * @param sc 
	 * @param input
	 * @param mcOut
	 * @param hasHeader
	 * @param delim
	 * @param fill
	 * @param fillValue
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> csvToBinaryBlock(JavaSparkContext sc,
			JavaRDD<String> input, MatrixCharacteristics mcOut, 
			boolean hasHeader, String delim, boolean fill, double fillValue) 
		throws DMLRuntimeException 
	{
		//convert string rdd to serializable longwritable/text
		JavaPairRDD<LongWritable, Text> prepinput =
				input.mapToPair(new StringToSerTextFunction());
		
		//convert to binary block
		return csvToBinaryBlock(sc, prepinput, mcOut, hasHeader, delim, fill, fillValue);
	}
	
	/**
	 * 
	 * @param in
	 * @return
	 */
	public static JavaPairRDD<LongWritable, Text> stringToSerializableText(JavaPairRDD<Long,String> in)
	{
		return in.mapToPair(new TextToSerTextFunction());
	}

	
	/////////////////////////////////
	// TEXTCELL-SPECIFIC FUNCTIONS
	
	/*private*/ static abstract class CellToBinaryBlockFunction implements Serializable
	{
		private static final long serialVersionUID = 4205331295408335933L;
		
		//internal buffer size (aligned w/ default matrix block size)
		protected static final int BUFFER_SIZE = 4 * 1000 * 1000; //4M elements (32MB)
		protected int _bufflen = -1;
		
		protected long _rlen = -1;
		protected long _clen = -1;
		protected int _brlen = -1;
		protected int _bclen = -1;
		
		protected CellToBinaryBlockFunction(MatrixCharacteristics mc)
		{
			_rlen = mc.getRows();
			_clen = mc.getCols();
			_brlen = mc.getRowsPerBlock();
			_bclen = mc.getColsPerBlock();
			
			//determine upper bounded buffer len
			_bufflen = (int) Math.min(_rlen*_clen, BUFFER_SIZE);
		}


		/**
		 * 
		 * @param rbuff
		 * @param ret
		 * @throws IOException 
		 * @throws DMLRuntimeException 
		 */
		protected void flushBufferToList( ReblockBuffer rbuff,  ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret ) 
			throws IOException, DMLRuntimeException
		{
			//temporary list of indexed matrix values to prevent library dependencies
			ArrayList<IndexedMatrixValue> rettmp = new ArrayList<IndexedMatrixValue>();					
			rbuff.flushBufferToBinaryBlocks(rettmp);
			ret.addAll(SparkUtils.fromIndexedMatrixBlock(rettmp));
		}
	}
	
	/**
	 * 
	 */
	private static class TextToSerTextFunction implements PairFunction<Tuple2<Long,String>,LongWritable,Text> 
	{
		private static final long serialVersionUID = 2286037080400222528L;

		@Override
		public Tuple2<LongWritable, Text> call(Tuple2<Long, String> arg0) 
			throws Exception 
		{
			SerLongWritable slarg = new SerLongWritable(arg0._1());
			SerText starg = new SerText(arg0._2());			
			return new Tuple2<LongWritable,Text>(slarg, starg);
		}
	}
	
	/**
	 * 
	 */
	private static class StringToSerTextFunction implements PairFunction<String, LongWritable, Text> 
	{
		private static final long serialVersionUID = 2286037080400222528L;

		@Override
		public Tuple2<LongWritable, Text> call(String arg0) 
			throws Exception 
		{
			SerLongWritable slarg = new SerLongWritable(1L);
			SerText starg = new SerText(arg0);			
			return new Tuple2<LongWritable,Text>(slarg, starg);
		}
	}
		
	/////////////////////////////////
	// CSV-SPECIFIC FUNCTIONS

	/**
	 * 
	 */
	private static class CSVAnalysisFunction implements Function<Text,String> 
	{
		private static final long serialVersionUID = 2310303223289674477L;

		private Accumulator<Double> _aNnz = null;
		private String _delim = null;
		
		public CSVAnalysisFunction( Accumulator<Double> aNnz, String delim )
		{
			_aNnz = aNnz;
			_delim = delim;
		}
		
		@Override
		public String call(Text v1) 
			throws Exception 
		{
			//parse input line
			String line = v1.toString();
			String[] cols = IOUtilFunctions.split(line, _delim);
			
			//determine number of non-zeros of row (w/o string parsing)
			long lnnz = 0;
			for( String col : cols ) {
				if( !col.isEmpty() && !col.equals("0") && !col.equals("0.0") ) {
					lnnz++;
				}
			}
			
			//update counters
			_aNnz.add( (double)lnnz );
			
			return line;
		}
		
	}
	
	/**
	 * 
	 */
	private static class ConcatenateBlocksFunction implements PairFunction<Tuple2<Long, Iterable<Tuple2<Long,MatrixBlock>>>,MatrixIndexes,MatrixBlock>
	{
		private static final long serialVersionUID = -7879603125149650097L;
		
		private long _clen = -1;
		private int _bclen = -1;
		private int _ncblks = -1;
		
		public ConcatenateBlocksFunction(long clen, int bclen) {
			_clen = clen;
			_bclen = bclen;
			_ncblks = (int)Math.ceil((double)clen/bclen);
		}
		
		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<Long,Iterable<Tuple2<Long, MatrixBlock>>> arg0)
			throws Exception 
		{
			long rowIndex = arg0._1();
			MatrixBlock[] tmpBlks = new MatrixBlock[_ncblks];
			
			//collect and sort input blocks
			Iterator<Tuple2<Long, MatrixBlock>> iter = arg0._2().iterator();
			while( iter.hasNext() ) {
				Tuple2<Long, MatrixBlock> entry = iter.next();
				tmpBlks[entry._1().intValue()-1] = entry._2();
			}
		
			//concatenate blocks
			MatrixBlock out = new MatrixBlock(1,(int)_clen, tmpBlks[0].isInSparseFormat());
			for( int i=0; i<_ncblks; i++ ) {				
				out.copy(0, 0, i*_bclen, (int)Math.min((i+1)*_bclen, _clen)-1, tmpBlks[i], false);				
			}
			out.recomputeNonZeros();
			
			//output row block
			return new Tuple2<MatrixIndexes,MatrixBlock>(new MatrixIndexes(rowIndex, 1),out);
		}		
	}
}
