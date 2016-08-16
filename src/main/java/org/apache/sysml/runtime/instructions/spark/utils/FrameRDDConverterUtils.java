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
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.spark.data.SerLongWritable;
import org.apache.sysml.runtime.instructions.spark.data.SerText;
import org.apache.sysml.runtime.instructions.spark.functions.ConvertFrameBlockToIJVLines;
import org.apache.sysml.runtime.instructions.spark.utils.FrameRDDConverterUtilsComp.BinaryBlockToCSVFunction;
import org.apache.sysml.runtime.instructions.spark.utils.FrameRDDConverterUtilsComp.BinaryBlockToMatrixBlockFunction;
import org.apache.sysml.runtime.instructions.spark.utils.FrameRDDConverterUtilsComp.CSVToBinaryBlockFunction;
import org.apache.sysml.runtime.instructions.spark.utils.FrameRDDConverterUtilsComp.MatrixToBinaryBlockFunction;
import org.apache.sysml.runtime.instructions.spark.utils.FrameRDDConverterUtilsComp.TextToBinaryBlockFunction;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.CSVFileFormatProperties;
import org.apache.sysml.runtime.matrix.data.FrameBlock;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.Pair;
import org.apache.sysml.runtime.matrix.mapred.FrameReblockBuffer;
import org.apache.sysml.runtime.transform.TfUtils;
import org.apache.sysml.runtime.util.DataConverter;



public class FrameRDDConverterUtils 
{
	//=====================================
	// CSV <--> Binary block

	/**
	 * 
	 * @param sc
	 * @param input
	 * @param mcOut
	 * @param hasHeader
	 * @param delim
	 * @param fill
	 * @param missingValue
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaPairRDD<Long, FrameBlock> csvToBinaryBlock(JavaSparkContext sc,
			JavaPairRDD<LongWritable, Text> input, MatrixCharacteristics mcOut, 
			boolean hasHeader, String delim, boolean fill, double fillValue) 
		throws DMLRuntimeException 
	{
		//determine unknown dimensions and sparsity if required
		if( !mcOut.dimsKnown(true) ) {
			JavaRDD<String> tmp = input.values()
					.map(new TextToStringFunction());
			String tmpStr = tmp.first();
			boolean metaHeader = tmpStr.startsWith(TfUtils.TXMTD_MVPREFIX) 
					|| tmpStr.startsWith(TfUtils.TXMTD_NDPREFIX);
			tmpStr = (metaHeader) ? tmpStr.substring(tmpStr.indexOf(delim)+1) : tmpStr;
			long rlen = tmp.count() - (hasHeader ? 1 : 0) - (metaHeader ? 2 : 0);
			long clen = IOUtilFunctions.splitCSV(tmpStr, delim).length;
			mcOut.set(rlen, clen, mcOut.getRowsPerBlock(), mcOut.getColsPerBlock(), -1);
		}
		
		//prepare csv w/ row indexes (sorted by filenames)
		JavaPairRDD<Text,Long> prepinput = input.values()
				.zipWithIndex(); //zip row index
		
		//convert csv rdd to binary block rdd (w/ partial blocks)
		JavaPairRDD<Long, FrameBlock> out = prepinput
				.mapPartitionsToPair(new CSVToBinaryBlockFunction(mcOut, hasHeader, delim, fill));
		
		return out;
	}
	
	/**
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
	public static JavaPairRDD<Long, FrameBlock> csvToBinaryBlock(JavaSparkContext sc,
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
	 * @param mcIn
	 * @param props
	 * @param strict
	 * @return
	 */
	public static JavaRDD<String> binaryBlockToCsv(JavaPairRDD<Long,FrameBlock> in, MatrixCharacteristics mcIn, CSVFileFormatProperties props, boolean strict)
	{
		JavaPairRDD<Long,FrameBlock> input = in;
		
		//sort if required (on blocks/rows)
		if( strict ) {
			input = input.sortByKey(true);
		}
		
		//convert binary block to csv (from blocks/rows)
		JavaRDD<String> out = input
				.flatMap(new BinaryBlockToCSVFunction(props));
	
		return out;
	}
	
	
	//=====================================
	// Text cell <--> Binary block
	
	/**
	 * 
	 * @param sc
	 * @param input
	 * @param mcOut
	 * @param schema
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaPairRDD<Long, FrameBlock> textCellToBinaryBlock(JavaSparkContext sc,
			JavaPairRDD<LongWritable, Text> in, MatrixCharacteristics mcOut, List<ValueType> schema ) 
		throws DMLRuntimeException  
	{
		//replicate schema entry if necessary
		List<ValueType> lschema = (schema.size()==1 && mcOut.getCols()>1) ?
				Collections.nCopies((int)mcOut.getCols(), schema.get(0)) : schema;
		
		//convert input rdd to serializable long/frame block
		JavaPairRDD<Long,Text> input = 
				in.mapToPair(new LongWritableTextToLongTextFunction());
		
		//do actual conversion
		return textCellToBinaryBlockLongIndex(sc, input, mcOut, lschema);
	}

	/**
	 * 
	 * @param sc
	 * @param input
	 * @param mcOut
	 * @param schema
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaPairRDD<Long, FrameBlock> textCellToBinaryBlockLongIndex(JavaSparkContext sc,
			JavaPairRDD<Long, Text> input, MatrixCharacteristics mcOut, List<ValueType> schema ) 
		throws DMLRuntimeException  
	{
		
 		//convert textcell rdd to binary block rdd (w/ partial blocks)
		JavaPairRDD<Long, FrameBlock> output = input.values().mapPartitionsToPair(new TextToBinaryBlockFunction( mcOut, schema ));
		
		//aggregate partial matrix blocks
		JavaPairRDD<Long,FrameBlock> out = 
				(JavaPairRDD<Long, FrameBlock>) RDDAggregateUtils.mergeByFrameKey( output ); 

		return out;
	}

	/**
	 * 
	 * @param input
	 * @param mcIn
	 * @param format
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaRDD<String> binaryBlockToTextCell(JavaPairRDD<Long, FrameBlock> input, MatrixCharacteristics mcIn) 
		throws DMLRuntimeException 
	{
		//convert frame blocks to ijv string triples  
		return input.flatMap(new ConvertFrameBlockToIJVLines());
	}
	
	//=====================================
	// Matrix block <--> Binary block

	/**
	 * 
	 * @param sc
	 * @param input
	 * @param mcIn
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaPairRDD<LongWritable, FrameBlock> matrixBlockToBinaryBlock(JavaSparkContext sc,
			JavaPairRDD<MatrixIndexes, MatrixBlock> input, MatrixCharacteristics mcIn)
		throws DMLRuntimeException 
	{
		//Do actual conversion
		JavaPairRDD<Long, FrameBlock> output = matrixBlockToBinaryBlockLongIndex(sc,input, mcIn);
		
		//convert input rdd to serializable LongWritable/frame block
		JavaPairRDD<LongWritable,FrameBlock> out = 
				output.mapToPair(new LongFrameToLongWritableFrameFunction());
		
		return out;
	}
	

	/**
	 * 
	 * @param sc
	 * @param input
	 * @param mcIn
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaPairRDD<Long, FrameBlock> matrixBlockToBinaryBlockLongIndex(JavaSparkContext sc,
			JavaPairRDD<MatrixIndexes, MatrixBlock> input, MatrixCharacteristics mcIn)
		throws DMLRuntimeException 
	{
		JavaPairRDD<Long, FrameBlock> out = null;
		
		if(mcIn.getCols() > mcIn.getColsPerBlock()) {
			
			out = input.flatMapToPair(new MatrixToBinaryBlockFunction(mcIn));
			
			//aggregate partial frame blocks
			if(mcIn.getCols() > mcIn.getColsPerBlock())
				out = (JavaPairRDD<Long, FrameBlock>) RDDAggregateUtils.mergeByFrameKey( out );
		}
		else
			out = input.mapToPair(new MatrixToBinaryBlockOneColumnBlockFunction(mcIn));
		
		return out;
	}
	

	
	/**
	 * 
	 * @param in
	 * @param mcIn
	 * @param props
	 * @param strict
	 * @return
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlockToMatrixBlock(JavaPairRDD<Long,FrameBlock> input, 
			MatrixCharacteristics mcIn, MatrixCharacteristics mcOut) 
	{
		//convert binary block to matrix block
		JavaPairRDD<MatrixIndexes, MatrixBlock> out = input
				.flatMapToPair(new BinaryBlockToMatrixBlockFunction(mcIn, mcOut));
	
		//aggregate partial matrix blocks
		out = RDDAggregateUtils.mergeByKey( out ); 	
		
		return out;
	}
	

	/////////////////////////////////
	// CSV-SPECIFIC FUNCTIONS
	
	/**
	 * 
	 */
	private static class StringToSerTextFunction implements PairFunction<String, LongWritable, Text> 
	{
		private static final long serialVersionUID = 8683232211035837695L;

		@Override
		public Tuple2<LongWritable, Text> call(String arg0) throws Exception {
			return new Tuple2<LongWritable,Text>(new SerLongWritable(1L), new SerText(arg0));
		}
	}
	
	/**
	 * 
	 */
	public static class LongWritableToSerFunction implements PairFunction<Tuple2<LongWritable,FrameBlock>,LongWritable,FrameBlock> 
	{
		private static final long serialVersionUID = 2286037080400222528L;
		
		@Override
		public Tuple2<LongWritable, FrameBlock> call(Tuple2<LongWritable, FrameBlock> arg0) throws Exception  {
			return new Tuple2<LongWritable,FrameBlock>(new SerLongWritable(arg0._1.get()), arg0._2);
		}
	}
	
	/**
	 * 
	 */
	public static class LongWritableTextToLongTextFunction implements PairFunction<Tuple2<LongWritable,Text>,Long,Text> 
	{
		private static final long serialVersionUID = -5408386071466175348L;

		@Override
		public Tuple2<Long, Text> call(Tuple2<LongWritable, Text> arg0) throws Exception  {
			return new Tuple2<Long,Text>(new Long(arg0._1.get()), arg0._2);
		}
	}
	
	/**
	 * 
	 */
	public static class LongFrameToLongWritableFrameFunction implements PairFunction<Tuple2<Long,FrameBlock>,LongWritable,FrameBlock> 
	{
		private static final long serialVersionUID = -1467314923206783333L;

		@Override
		public Tuple2<LongWritable, FrameBlock> call(Tuple2<Long, FrameBlock> arg0) throws Exception  {
			return new Tuple2<LongWritable, FrameBlock>(new LongWritable(arg0._1), arg0._2);
		}
	}

	/**
	 * 
	 */
	public static class LongWritableFrameToLongFrameFunction implements PairFunction<Tuple2<LongWritable,FrameBlock>,Long,FrameBlock> 
	{
		private static final long serialVersionUID = -1232439643533739078L;

		@Override
		public Tuple2<Long, FrameBlock> call(Tuple2<LongWritable, FrameBlock> arg0) throws Exception  {
			return new Tuple2<Long, FrameBlock>(arg0._1.get(), arg0._2);
		}
	}
	
	/**
	 * 
	 */
	private static class TextToStringFunction implements Function<Text,String> 
	{
		private static final long serialVersionUID = -2744814934501782747L;

		@Override
		public String call(Text v1) throws Exception {
			return v1.toString();
		}
	}

	
	/////////////////////////////////
	// TEXTCELL-SPECIFIC FUNCTIONS
	
	/*private*/ static abstract class CellToBinaryBlockFunction implements Serializable
	{
		private static final long serialVersionUID = -729614449626680946L;

		protected int _bufflen = -1;
		protected long _rlen = -1;
		protected long _clen = -1;
		
		protected CellToBinaryBlockFunction(MatrixCharacteristics mc)
		{
			_rlen = mc.getRows();
			_clen = mc.getCols();
			
			//determine upper bounded buffer len
			_bufflen = (int) Math.min(_rlen*_clen, FrameBlock.BUFFER_SIZE);
		}


		/**
		 * 
		 * @param rbuff
		 * @param ret
		 * @throws IOException 
		 * @throws DMLRuntimeException 
		 */
		protected void flushBufferToList( FrameReblockBuffer rbuff,  ArrayList<Tuple2<Long,FrameBlock>> ret ) 
			throws IOException, DMLRuntimeException
		{
			//temporary list of indexed matrix values to prevent library dependencies
			ArrayList<Pair<Long, FrameBlock>> rettmp = new ArrayList<Pair<Long, FrameBlock>>();
			rbuff.flushBufferToBinaryBlocks(rettmp);
			ret.addAll(SparkUtils.fromIndexedFrameBlock(rettmp));
		}
	}
	
	

	/*
	 * This function supports if matrix has only one column block.
	 */
	private static class MatrixToBinaryBlockOneColumnBlockFunction implements PairFunction<Tuple2<MatrixIndexes,MatrixBlock>,Long,FrameBlock>
	{
		private static final long serialVersionUID = 3716019666116660815L;

		private int _brlen = -1;
		private int _bclen = -1;
		private long _clen = -1;
	
		
		public MatrixToBinaryBlockOneColumnBlockFunction(MatrixCharacteristics mc)
		{
			_brlen = mc.getRowsPerBlock();
			_bclen = mc.getColsPerBlock();
			_clen = mc.getCols();
		}

		@Override
		public Tuple2<Long, FrameBlock> call(Tuple2<MatrixIndexes,MatrixBlock> arg0) 
			throws Exception 
		{
			if(_clen > _bclen)
				throw new DMLRuntimeException("The input matrix has more than one column block, this function supports only one column block.");

			MatrixIndexes matrixIndexes = arg0._1();
			MatrixBlock matrixBlock = arg0._2();
			
			//Frame Index (Row id, with base 1)
			Long rowix = new Long((matrixIndexes.getRowIndex()-1)*_brlen+1);

			FrameBlock frameBlock = DataConverter.convertToFrameBlock(matrixBlock);
			return new Tuple2<Long, FrameBlock>(rowix, frameBlock);
		}
	}

	
	//////////////////////////////////////
	// Common functions
	
	// Flushes current state of filled column blocks to output list.
	/*private*/ static void flushBlocksToList( Long[] ix, FrameBlock[] mb, ArrayList<Tuple2<Long,FrameBlock>> ret ) 
		throws DMLRuntimeException
	{
		int len = ix.length;			
		for( int i=0; i<len; i++ )
			if( mb[i] != null ) {
				ret.add(new Tuple2<Long,FrameBlock>(ix[i],mb[i]));
			}	
	}
}
