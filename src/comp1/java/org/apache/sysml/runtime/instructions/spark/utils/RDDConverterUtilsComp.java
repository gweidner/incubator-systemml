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

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtils.CellToBinaryBlockFunction;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.CSVFileFormatProperties;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixCell;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.SparseBlock;
import org.apache.sysml.runtime.matrix.mapred.ReblockBuffer;
import org.apache.sysml.runtime.util.DataConverter;
import org.apache.sysml.runtime.util.FastStringTokenizer;
import org.apache.sysml.runtime.util.UtilFunctions;

public class RDDConverterUtilsComp 
{
		
	/////////////////////////////////
	// BINARYBLOCK-SPECIFIC FUNCTIONS

	/**
	 * This function converts a binary block input (<X,y>) into mllib's labeled points. Note that
	 * this function requires prior reblocking if the number of columns is larger than the column
	 * block size. 
	 */
	/*private*/ static class PrepareBinaryBlockFunction implements FlatMapFunction<MatrixBlock, LabeledPoint> 
	{
		private static final long serialVersionUID = -6590259914203201585L;

		@Override
		public Iterable<LabeledPoint> call(MatrixBlock arg0) 
			throws Exception 
		{
			ArrayList<LabeledPoint> ret = new ArrayList<LabeledPoint>();
			for( int i=0; i<arg0.getNumRows(); i++ )
			{
				MatrixBlock tmp = arg0.sliceOperations(i, i, 0, arg0.getNumColumns()-2, new MatrixBlock());
				double[] data = DataConverter.convertToDoubleVector(tmp);
				if( tmp.isEmptyBlock(false) ) //EMPTY SPARSE ROW
				{
					ret.add(new LabeledPoint(arg0.getValue(i, arg0.getNumColumns()-1), Vectors.sparse(0, new int[0], new double[0])));
				}
				else if( tmp.isInSparseFormat() ) //SPARSE ROW
				{
					SparseBlock sblock = tmp.getSparseBlock();
					ret.add(new LabeledPoint(arg0.getValue(i, arg0.getNumColumns()-1), 
							Vectors.sparse(sblock.size(0), sblock.indexes(0), sblock.values(0))));
				}
				else // DENSE ROW
				{
					ret.add(new LabeledPoint(arg0.getValue(i, arg0.getNumColumns()-1), Vectors.dense(data)));
				}
			}
			
			return ret;
		}
	}
	
	/////////////////////////////////
	// TEXTCELL-SPECIFIC FUNCTIONS
	
	/**
	 * 
	 */
	/*private*/ static class TextToBinaryBlockFunction extends CellToBinaryBlockFunction implements PairFlatMapFunction<Iterator<Text>,MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = 4907483236186747224L;

		protected TextToBinaryBlockFunction(MatrixCharacteristics mc) {
			super(mc);
		}

		@Override
		public Iterable<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Text> arg0) 
			throws Exception 
		{
			ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();
			ReblockBuffer rbuff = new ReblockBuffer(_bufflen, _rlen, _clen, _brlen, _bclen);
			FastStringTokenizer st = new FastStringTokenizer(' ');
			
			while( arg0.hasNext() )
			{
				//get input string (ignore matrix market comments)
				String strVal = arg0.next().toString();
				if( strVal.startsWith("%") ) 
					continue;
				
				//parse input ijv triple
				st.reset( strVal );
				long row = st.nextLong();
				long col = st.nextLong();
				double val = st.nextDouble();
				
				//flush buffer if necessary
				if( rbuff.getSize() >= rbuff.getCapacity() )
					flushBufferToList(rbuff, ret);
				
				//add value to reblock buffer
				rbuff.appendCell(row, col, val);
			}
			
			//final flush buffer
			flushBufferToList(rbuff, ret);
		
			return ret;
		}
	}
	
	/////////////////////////////////
	// BINARYCELL-SPECIFIC FUNCTIONS

	/*private*/ static class BinaryCellToBinaryBlockFunction extends CellToBinaryBlockFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes,MatrixCell>>,MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = 3928810989462198243L;

		protected BinaryCellToBinaryBlockFunction(MatrixCharacteristics mc) {
			super(mc);
		}

		@Override
		public Iterable<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes,MatrixCell>> arg0) 
			throws Exception 
		{
			ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();
			ReblockBuffer rbuff = new ReblockBuffer(_bufflen, _rlen, _clen, _brlen, _bclen);
			
			while( arg0.hasNext() )
			{
				//unpack the binary cell input
				Tuple2<MatrixIndexes,MatrixCell> tmp = arg0.next();
				
				//parse input ijv triple
				long row = tmp._1().getRowIndex();
				long col = tmp._1().getColumnIndex();
				double val = tmp._2().getValue();
				
				//flush buffer if necessary
				if( rbuff.getSize() >= rbuff.getCapacity() )
					flushBufferToList(rbuff, ret);
				
				//add value to reblock buffer
				rbuff.appendCell(row, col, val);
			}
			
			//final flush buffer
			flushBufferToList(rbuff, ret);
		
			return ret;
		}
	}
	
	/////////////////////////////////
	// CSV-SPECIFIC FUNCTIONS


	/**
	 * This functions allows to map rdd partitions of csv rows into a set of partial binary blocks.
	 * 
	 * NOTE: For this csv to binary block function, we need to hold all output blocks per partition 
	 * in-memory. Hence, we keep state of all column blocks and aggregate row segments into these blocks. 
	 * In terms of memory consumption this is better than creating partial blocks of row segments.
	 * 
	 */
	/*private*/ static class CSVToBinaryBlockFunction implements PairFlatMapFunction<Iterator<Tuple2<Text,Long>>,MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = -4948430402942717043L;
		
		private long _rlen = -1;
		private long _clen = -1;
		private int _brlen = -1;
		private int _bclen = -1;
		private boolean _header = false;
		private String _delim = null;
		private boolean _fill = false;
		private double _fillValue = 0;
		
		public CSVToBinaryBlockFunction(MatrixCharacteristics mc, boolean hasHeader, String delim, boolean fill, double fillValue)
		{
			_rlen = mc.getRows();
			_clen = mc.getCols();
			_brlen = mc.getRowsPerBlock();
			_bclen = mc.getColsPerBlock();
			_header = hasHeader;
			_delim = delim;
			_fill = fill;
			_fillValue = fillValue;
		}

		@Override
		public Iterable<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<Text,Long>> arg0) 
			throws Exception 
		{
			ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();

			int ncblks = (int)Math.ceil((double)_clen/_bclen);
			MatrixIndexes[] ix = new MatrixIndexes[ncblks];
			MatrixBlock[] mb = new MatrixBlock[ncblks];
			
			while( arg0.hasNext() )
			{
				Tuple2<Text,Long> tmp = arg0.next();
				String row = tmp._1().toString();
				long rowix = tmp._2() + (_header ? 0 : 1);
				
				//skip existing header
				if( _header && rowix == 0  ) 
					continue;
				
				long rix = UtilFunctions.computeBlockIndex(rowix, _brlen);
				int pos = UtilFunctions.computeCellInBlock(rowix, _brlen);
			
				//create new blocks for entire row
				if( ix[0] == null || ix[0].getRowIndex() != rix ) {
					if( ix[0] !=null )
						flushBlocksToList(ix, mb, ret);
					long len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);
					createBlocks(rowix, (int)len, ix, mb);
				}
				
				//process row data
				String[] parts = IOUtilFunctions.split(row, _delim);
				boolean emptyFound = false;
				for( int cix=1, pix=0; cix<=ncblks; cix++ ) 
				{
					int lclen = (int)UtilFunctions.computeBlockSize(_clen, cix, _bclen);				
					for( int j=0; j<lclen; j++ ) {
						String part = parts[pix++];
						emptyFound |= part.isEmpty() && !_fill;
						double val = (part.isEmpty() && _fill) ?
								_fillValue : Double.parseDouble(part);
						mb[cix-1].appendValue(pos, j, val);
					}	
				}
		
				//sanity check empty cells filled w/ values
				IOUtilFunctions.checkAndRaiseErrorCSVEmptyField(row, _fill, emptyFound);
			}
		
			//flush last blocks
			flushBlocksToList(ix, mb, ret);
		
			return ret;
		}
		
		// Creates new state of empty column blocks for current global row index.
		private void createBlocks(long rowix, int lrlen, MatrixIndexes[] ix, MatrixBlock[] mb)
		{
			//compute row block index and number of column blocks
			long rix = UtilFunctions.computeBlockIndex(rowix, _brlen);
			int ncblks = (int)Math.ceil((double)_clen/_bclen);
			
			//create all column blocks (assume dense since csv is dense text format)
			for( int cix=1; cix<=ncblks; cix++ ) {
				int lclen = (int)UtilFunctions.computeBlockSize(_clen, cix, _bclen);				
				ix[cix-1] = new MatrixIndexes(rix, cix);
				mb[cix-1] = new MatrixBlock(lrlen, lclen, false);		
			}
		}
		
		// Flushes current state of filled column blocks to output list.
		private void flushBlocksToList( MatrixIndexes[] ix, MatrixBlock[] mb, ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret ) 
			throws DMLRuntimeException
		{
			int len = ix.length;			
			for( int i=0; i<len; i++ )
				if( mb[i] != null ) {
					ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ix[i],mb[i]));
					mb[i].examSparsity(); //ensure right representation
				}	
		}
	}
	
	/**
	 * 
	 */
	/*private*/ static class BinaryBlockToCSVFunction implements FlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>,String> 
	{
		private static final long serialVersionUID = 1891768410987528573L;

		private CSVFileFormatProperties _props = null;
		
		public BinaryBlockToCSVFunction(CSVFileFormatProperties props) {
			_props = props;
		}

		@Override
		public Iterable<String> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
			throws Exception 
		{
			MatrixIndexes ix = arg0._1();
			MatrixBlock blk = arg0._2();
			
			ArrayList<String> ret = new ArrayList<String>();
			
			//handle header information
			if(_props.hasHeader() && ix.getRowIndex()==1 ) {
				StringBuilder sb = new StringBuilder();
	    		for(int j = 1; j < blk.getNumColumns(); j++) {
	    			if(j != 1)
	    				sb.append(_props.getDelim());
	    			sb.append("C" + j);
	    		}
    			ret.add(sb.toString());
	    	}
		
			//handle matrix block data
			StringBuilder sb = new StringBuilder();
    		for(int i=0; i<blk.getNumRows(); i++) {
    			for(int j=0; j<blk.getNumColumns(); j++) {
	    			if(j != 0)
	    				sb.append(_props.getDelim());
	    			double val = blk.quickGetValue(i, j);
	    			if(!(_props.isSparse() && val == 0))
	    				sb.append(val);
				}
	    		ret.add(sb.toString());
	    		sb.setLength(0); //reset
    		}
			
			return ret;
		}
	}
	
	/**
	 * 
	 */
	/*private*/ static class SliceBinaryBlockToRowsFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>,Long,Tuple2<Long,MatrixBlock>> 
	{
		private static final long serialVersionUID = 7192024840710093114L;
		
		private int _brlen = -1;
		
		public SliceBinaryBlockToRowsFunction(int brlen) {
			_brlen = brlen;
		}
		
		@Override
		public Iterable<Tuple2<Long,Tuple2<Long,MatrixBlock>>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) 
			throws Exception 
		{
			ArrayList<Tuple2<Long,Tuple2<Long,MatrixBlock>>> ret = 
					new ArrayList<Tuple2<Long,Tuple2<Long,MatrixBlock>>>();
			
			MatrixIndexes ix = arg0._1();
			MatrixBlock blk = arg0._2();
			
			for( int i=0; i<blk.getNumRows(); i++ ) {
				MatrixBlock tmpBlk = blk.sliceOperations(i, i, 0, blk.getNumColumns()-1, new MatrixBlock());
				long rix = UtilFunctions.computeCellIndex(ix.getRowIndex(), _brlen, i);
				ret.add(new Tuple2<Long,Tuple2<Long,MatrixBlock>>(rix, 
						new Tuple2<Long,MatrixBlock>(ix.getColumnIndex(),tmpBlk)));
			}
			
			return ret;
		}
	}
}
