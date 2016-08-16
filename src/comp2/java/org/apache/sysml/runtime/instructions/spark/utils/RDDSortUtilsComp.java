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

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import org.apache.sysml.runtime.instructions.spark.data.PartitionedBlock;
import org.apache.sysml.runtime.instructions.spark.data.RowMatrixBlock;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtils.DoublePair;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtils.ValueIndexPair;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.util.DataConverter;
import org.apache.sysml.runtime.util.UtilFunctions;

/**
 * 
 */
public class RDDSortUtilsComp 
{
	
	/**
	 * 
	 */
	/*private*/ static class ExtractDoubleValuesFunction implements FlatMapFunction<MatrixBlock,Double> 
	{
		private static final long serialVersionUID = 6888003502286282876L;

		@Override
		public Iterator<Double> call(MatrixBlock arg0) 
			throws Exception 
		{
			return DataConverter.convertToDoubleList(arg0).iterator();
		}		
	}

	/**
	 * 
	 */
	/*private*/ static class ExtractDoubleValuesFunction2 implements FlatMapFunction<Tuple2<MatrixBlock,MatrixBlock>,DoublePair> 
	{
		private static final long serialVersionUID = 2132672563825289022L;

		@Override
		public Iterator<DoublePair> call(Tuple2<MatrixBlock,MatrixBlock> arg0) 
			throws Exception 
		{
			ArrayList<DoublePair> ret = new ArrayList<DoublePair>(); 
			MatrixBlock mb1 = arg0._1();
			MatrixBlock mb2 = arg0._2();
			
			for( int i=0; i<mb1.getNumRows(); i++) {
				ret.add(new DoublePair(
						mb1.quickGetValue(i, 0),
						mb2.quickGetValue(i, 0)));
			}
			
			return ret.iterator();
		}		
	}
	
	/*private*/ static class ExtractDoubleValuesWithIndexFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>,ValueIndexPair,Double> 
	{
		private static final long serialVersionUID = -3976735381580482118L;
		
		private int _brlen = -1;
		
		public ExtractDoubleValuesWithIndexFunction(int brlen)
		{
			_brlen = brlen;
		}
		
		@Override
		public Iterator<Tuple2<ValueIndexPair,Double>> call(Tuple2<MatrixIndexes,MatrixBlock> arg0) 
			throws Exception 
		{
			ArrayList<Tuple2<ValueIndexPair,Double>> ret = new ArrayList<Tuple2<ValueIndexPair,Double>>(); 
			MatrixIndexes ix = arg0._1();
			MatrixBlock mb = arg0._2();
			
			long ixoffset = (ix.getRowIndex()-1)*_brlen;
			for( int i=0; i<mb.getNumRows(); i++) {
				double val = mb.quickGetValue(i, 0);
				ret.add(new Tuple2<ValueIndexPair,Double>(
						new ValueIndexPair(val,ixoffset+i+1), val));
			}
			
			return ret.iterator();
		}		
	}
	
	/**
	 * 
	 */
	/*private*/ static class ConvertToBinaryBlockFunction implements PairFlatMapFunction<Iterator<Tuple2<Double,Long>>,MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = 5000298196472931653L;
		
		private long _rlen = -1;
		private int _brlen = -1;
		
		public ConvertToBinaryBlockFunction(long rlen, int brlen)
		{
			_rlen = rlen;
			_brlen = brlen;
		}
		
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<Double,Long>> arg0) 
			throws Exception 
		{
			ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();
			
			MatrixIndexes ix = null;
			MatrixBlock mb = null;
			
			while( arg0.hasNext() ) 
			{
				Tuple2<Double,Long> val = arg0.next();
				long valix = val._2 + 1;
				long rix = UtilFunctions.computeBlockIndex(valix, _brlen);
				int pos = UtilFunctions.computeCellInBlock(valix, _brlen);
				
				if( ix == null || ix.getRowIndex() != rix )
				{
					if( ix !=null )
						ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ix,mb));
					long len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);
					ix = new MatrixIndexes(rix,1);
					mb = new MatrixBlock((int)len, 1, false);	
				}
				
				mb.quickSetValue(pos, 0, val._1);
			}
			
			//flush last block
			if( mb!=null && mb.getNonZeros() != 0 )
				ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ix,mb));
			
			return ret.iterator();
		}
	}

	/**
	 * 
	 */
	/*private*/ static class ConvertToBinaryBlockFunction2 implements PairFlatMapFunction<Iterator<Tuple2<DoublePair,Long>>,MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = -8638434373377180192L;
		
		private long _rlen = -1;
		private int _brlen = -1;
		
		public ConvertToBinaryBlockFunction2(long rlen, int brlen)
		{
			_rlen = rlen;
			_brlen = brlen;
		}
		
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<DoublePair,Long>> arg0) 
			throws Exception
		{
			ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();
			
			MatrixIndexes ix = null;
			MatrixBlock mb = null;
			
			while( arg0.hasNext() ) 
			{
				Tuple2<DoublePair,Long> val = arg0.next();
				long valix = val._2 + 1;
				long rix = UtilFunctions.computeBlockIndex(valix, _brlen);
				int pos = UtilFunctions.computeCellInBlock(valix, _brlen);
				
				if( ix == null || ix.getRowIndex() != rix )
				{
					if( ix !=null )
						ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ix,mb));
					long len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);
					ix = new MatrixIndexes(rix,1);
					mb = new MatrixBlock((int)len, 2, false);	
				}
				
				mb.quickSetValue(pos, 0, val._1.val1);
				mb.quickSetValue(pos, 1, val._1.val2);
			}
			
			//flush last block
			if( mb!=null && mb.getNonZeros() != 0 )
				ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ix,mb));
			
			return ret.iterator();
		}
	}
	
	/**
	 * 
	 */
	/*private*/ static class ConvertToBinaryBlockFunction3 implements PairFlatMapFunction<Iterator<Tuple2<ValueIndexPair,Long>>,MatrixIndexes,MatrixBlock> 
	{		
		private static final long serialVersionUID = 9113122668214965797L;
		
		private long _rlen = -1;
		private int _brlen = -1;
		
		public ConvertToBinaryBlockFunction3(long rlen, int brlen)
		{
			_rlen = rlen;
			_brlen = brlen;
		}
		
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<ValueIndexPair,Long>> arg0) 
			throws Exception
		{
			ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();
			
			MatrixIndexes ix = null;
			MatrixBlock mb = null;
			
			while( arg0.hasNext() ) 
			{
				Tuple2<ValueIndexPair,Long> val = arg0.next();
				long valix = val._2 + 1;
				long rix = UtilFunctions.computeBlockIndex(valix, _brlen);
				int pos = UtilFunctions.computeCellInBlock(valix, _brlen);
				
				if( ix == null || ix.getRowIndex() != rix )
				{
					if( ix !=null )
						ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ix,mb));
					long len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);
					ix = new MatrixIndexes(rix,1);
					mb = new MatrixBlock((int)len, 1, false);	
				}
				
				mb.quickSetValue(pos, 0, val._1.ix);
			}
			
			//flush last block
			if( mb!=null && mb.getNonZeros() != 0 )
				ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ix,mb));
			
			return ret.iterator();
		}
	}
	
	/**
	 * 
	 */
	/*private*/ static class ConvertToBinaryBlockFunction4 implements PairFlatMapFunction<Iterator<Tuple2<Long,Long>>,MatrixIndexes,MatrixBlock> 
	{	
		private static final long serialVersionUID = 9113122668214965797L;
		
		private long _rlen = -1;
		private int _brlen = -1;
		
		public ConvertToBinaryBlockFunction4(long rlen, int brlen)
		{
			_rlen = rlen;
			_brlen = brlen;
		}
		
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<Long,Long>> arg0) 
			throws Exception
		{
			ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();
			
			MatrixIndexes ix = null;
			MatrixBlock mb = null;
			
			while( arg0.hasNext() ) 
			{
				Tuple2<Long,Long> val = arg0.next();
				long valix = val._1;
				long rix = UtilFunctions.computeBlockIndex(valix, _brlen);
				int pos = UtilFunctions.computeCellInBlock(valix, _brlen);
				
				if( ix == null || ix.getRowIndex() != rix )
				{
					if( ix !=null )
						ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ix,mb));
					long len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);
					ix = new MatrixIndexes(rix,1);
					mb = new MatrixBlock((int)len, 1, false);	
				}
				
				mb.quickSetValue(pos, 0, val._2+1);
			}
			
			//flush last block
			if( mb!=null && mb.getNonZeros() != 0 )
				ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ix,mb));
			
			return ret.iterator();
		}
	}
	
	/*private*/ static class ShuffleMatrixBlockRowsFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes,Tuple2<MatrixBlock,MatrixBlock>>>,MatrixIndexes,RowMatrixBlock> 
	{	
		private static final long serialVersionUID = 6885207719329119646L;
		
		private long _rlen = -1;
		private int _brlen = -1;
		
		public ShuffleMatrixBlockRowsFunction(long rlen, int brlen)
		{
			_rlen = rlen;
			_brlen = brlen;
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, RowMatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, Tuple2<MatrixBlock, MatrixBlock>>> arg0)
			throws Exception 
		{
			return new ShuffleMatrixIterator(arg0);
		}
		
		/**
		 * Lazy iterator to prevent blk output for better resource efficiency; 
		 * This also lowered garbage collection overhead.
		 */
		private class ShuffleMatrixIterator implements Iterable<Tuple2<MatrixIndexes, RowMatrixBlock>>, Iterator<Tuple2<MatrixIndexes, RowMatrixBlock>>
		{
			private Iterator<Tuple2<MatrixIndexes, Tuple2<MatrixBlock, MatrixBlock>>> _inIter = null;
			private Tuple2<MatrixIndexes, Tuple2<MatrixBlock, MatrixBlock>> _currBlk = null;
			private int _currPos = -1;
			
			public ShuffleMatrixIterator(Iterator<Tuple2<MatrixIndexes, Tuple2<MatrixBlock, MatrixBlock>>> in) {
				_inIter = in;
			}

			public Iterator<Tuple2<MatrixIndexes, RowMatrixBlock>> iterator() {
				return this;
			}

			@Override
			public boolean hasNext() {
				return _currBlk != null || _inIter.hasNext();
			}
			
			@Override
			public Tuple2<MatrixIndexes, RowMatrixBlock> next() 
			{
				//pull next input block (if required)
				if( _currBlk == null ){
					_currBlk = _inIter.next();
					_currPos = 0;
				}
				
				try
				{
					//produce next output tuple
					MatrixIndexes ixmap = _currBlk._1();
					MatrixBlock data = _currBlk._2()._1();
					MatrixBlock mbTargetIndex = _currBlk._2()._2();
					
					long valix = (long) mbTargetIndex.getValue(_currPos, 0);
					long rix = UtilFunctions.computeBlockIndex(valix, _brlen);
					int pos = UtilFunctions.computeCellInBlock(valix, _brlen);
					int len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);		
					MatrixIndexes lix = new MatrixIndexes(rix,ixmap.getColumnIndex());
					MatrixBlock tmp = data.sliceOperations(_currPos, _currPos, 0, data.getNumColumns()-1, new MatrixBlock());
					_currPos++;
					
					//handle end of block situations
					if( _currPos == data.getNumRows() ){
						_currBlk = null;
					}
					
					return new Tuple2<MatrixIndexes,RowMatrixBlock>(lix, new RowMatrixBlock(len, pos, tmp));
				}
				catch(Exception ex) {
					throw new RuntimeException(ex);
				}
			}

			@Override
			public void remove() {
				throw new RuntimeException("Unsupported remove operation.");
			}
		}
	}
	
	/*private*/ static class ShuffleMatrixBlockRowsInMemFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes,MatrixBlock>>,MatrixIndexes,RowMatrixBlock> 
	{	
		private static final long serialVersionUID = 6885207719329119646L; 
		
		private long _rlen = -1;
		private int _brlen = -1;

		private Broadcast<PartitionedBlock<MatrixBlock>> _pmb = null;
		
		public ShuffleMatrixBlockRowsInMemFunction(long rlen, int brlen, Broadcast<PartitionedBlock<MatrixBlock>> pmb)
		{
			_rlen = rlen;
			_brlen = brlen;
			_pmb = pmb;
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, RowMatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> arg0)
			throws Exception 
		{
			return new ShuffleMatrixIterator(arg0);
		}
		
		/**
		 * Lazy iterator to prevent blk output for better resource efficiency; 
		 * This also lowered garbage collection overhead.
		 */
		private class ShuffleMatrixIterator implements Iterable<Tuple2<MatrixIndexes, RowMatrixBlock>>, Iterator<Tuple2<MatrixIndexes, RowMatrixBlock>>
		{
			private Iterator<Tuple2<MatrixIndexes, MatrixBlock>> _inIter = null;
			private Tuple2<MatrixIndexes, MatrixBlock> _currBlk = null;
			private int _currPos = -1;
			
			public ShuffleMatrixIterator(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> in) {
				_inIter = in;
			}

			public Iterator<Tuple2<MatrixIndexes, RowMatrixBlock>> iterator() {
				return this;
			}

			@Override
			public boolean hasNext() {
				return _currBlk != null || _inIter.hasNext();
			}
			
			@Override
			public Tuple2<MatrixIndexes, RowMatrixBlock> next() 
			{
				//pull next input block (if required)
				if( _currBlk == null ){
					_currBlk = _inIter.next();
					_currPos = 0;
				}
				
				try
				{
					//produce next output tuple
					MatrixIndexes ixmap = _currBlk._1();
					MatrixBlock data = _currBlk._2();
					MatrixBlock mbTargetIndex = _pmb.value().getBlock((int)ixmap.getRowIndex(), 1);
					
					long valix = (long) mbTargetIndex.getValue(_currPos, 0);
					long rix = UtilFunctions.computeBlockIndex(valix, _brlen);
					int pos = UtilFunctions.computeCellInBlock(valix, _brlen);
					int len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);		
					MatrixIndexes lix = new MatrixIndexes(rix,ixmap.getColumnIndex());
					MatrixBlock tmp = data.sliceOperations(_currPos, _currPos, 0, data.getNumColumns()-1, new MatrixBlock());
					_currPos++;
					
					//handle end of block situations
					if( _currPos == data.getNumRows() ){
						_currBlk = null;
					}
					
					return new Tuple2<MatrixIndexes,RowMatrixBlock>(lix, new RowMatrixBlock(len, pos, tmp));
				}
				catch(Exception ex) {
					throw new RuntimeException(ex);
				}
			}

			@Override
			public void remove() {
				throw new RuntimeException("Unsupported remove operation.");
			}
		}
	}
}
