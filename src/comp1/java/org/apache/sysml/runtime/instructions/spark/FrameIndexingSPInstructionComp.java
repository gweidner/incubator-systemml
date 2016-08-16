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

package org.apache.sysml.runtime.instructions.spark;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.FrameBlock;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.data.Pair;
import org.apache.sysml.runtime.util.IndexRange;
import org.apache.sysml.runtime.util.UtilFunctions;

public class FrameIndexingSPInstructionComp
{
	/**
	 * 
	 */
	/*private*/ static class SliceRHSForLeftIndexing implements PairFlatMapFunction<Tuple2<Long,FrameBlock>, Long, FrameBlock> 
	{
		private static final long serialVersionUID = 5724800998701216440L;

		private IndexRange _ixrange = null; 
		private int _brlen = -1; 
		private int _bclen = -1;
		private long _rlen = -1;
		private long _clen = -1;
		
		public SliceRHSForLeftIndexing(IndexRange ixrange, MatrixCharacteristics mcLeft) {
			_ixrange = ixrange;
			_rlen = mcLeft.getRows();
			_clen = mcLeft.getCols();
			_brlen = (int) Math.min(OptimizerUtils.getDefaultFrameSize(), _rlen);
			_bclen = (int) mcLeft.getCols();
		}

		@Override
		public Iterable<Tuple2<Long, FrameBlock>> call(Tuple2<Long, FrameBlock> rightKV) 
			throws Exception 
		{
			Pair<Long,FrameBlock> in = SparkUtils.toIndexedFrameBlock(rightKV);			
			ArrayList<Pair<Long,FrameBlock>> out = new ArrayList<Pair<Long,FrameBlock>>();
			OperationsOnMatrixValues.performShift(in, _ixrange, _brlen, _bclen, _rlen, _clen, out);
			return SparkUtils.fromIndexedFrameBlock(out);
		}		
	}
	
	/**
	 * 
	 */
	/*private*/ static class ZeroOutLHS implements PairFlatMapFunction<Tuple2<Long,FrameBlock>, Long,FrameBlock> 
	{
		private static final long serialVersionUID = -2672267231152496854L;

		private boolean _complement = false;
		private IndexRange _ixrange = null;
		private int _brlen = -1;
		private int _bclen = -1;
		private long _rlen = -1;
		
		public ZeroOutLHS(boolean complement, IndexRange range, MatrixCharacteristics mcLeft) {
			_complement = complement;
			_ixrange = range;
			_brlen = (int) OptimizerUtils.getDefaultFrameSize();
			_bclen = (int) mcLeft.getCols();
			_rlen = mcLeft.getRows();
		}
		
		@Override
		public Iterable<Tuple2<Long, FrameBlock>> call(Tuple2<Long, FrameBlock> kv) 
			throws Exception 
		{
			ArrayList<Pair<Long,FrameBlock>> out = new ArrayList<Pair<Long,FrameBlock>>();

			IndexRange curBlockRange = new IndexRange(_ixrange.rowStart, _ixrange.rowEnd, _ixrange.colStart, _ixrange.colEnd);
			
			// Global index of row (1-based)
			long lGblStartRow = ((kv._1.longValue()-1)/_brlen)*_brlen+1;
			FrameBlock zeroBlk = null;
			int iMaxRowsToCopy = 0;
			
			// Starting local location (0-based) of target block where to start copy. 
			int iRowStartDest = UtilFunctions.computeCellInBlock(kv._1, _brlen);
			for(int iRowStartSrc = 0; iRowStartSrc<kv._2.getNumRows(); iRowStartSrc += iMaxRowsToCopy, lGblStartRow += _brlen) {
				IndexRange range = UtilFunctions.getSelectedRangeForZeroOut(new Pair<Long, FrameBlock>(kv._1, kv._2), _brlen, _bclen, curBlockRange, lGblStartRow-1, lGblStartRow);
				if(range.rowStart == -1 && range.rowEnd == -1 && range.colStart == -1 && range.colEnd == -1) {
					throw new Exception("Error while getting range for zero-out");
				}
				//Maximum range of rows in target block 
				int iMaxRows=(int) Math.min(_brlen, _rlen-lGblStartRow+1);
				
				// Maximum number of rows to be copied from source block to target.
				iMaxRowsToCopy = Math.min(iMaxRows, kv._2.getNumRows()-iRowStartSrc);
				iMaxRowsToCopy = Math.min(iMaxRowsToCopy, iMaxRows-iRowStartDest);
				
				// Zero out the applicable range in this block
				zeroBlk = (FrameBlock) kv._2.zeroOutOperations(new FrameBlock(), range, _complement, iRowStartSrc, iRowStartDest, iMaxRows, iMaxRowsToCopy);
				out.add(new Pair<Long, FrameBlock>(lGblStartRow, zeroBlk));
				curBlockRange.rowStart =  lGblStartRow + _brlen;
				iRowStartDest = UtilFunctions.computeCellInBlock(iRowStartDest+iMaxRowsToCopy+1, _brlen);
			}
			return SparkUtils.fromIndexedFrameBlock(out);
		}
	}
	
	
	/**
	 * 
	 */
	/*private*/ static class SliceBlock implements PairFlatMapFunction<Tuple2<Long, FrameBlock>, Long, FrameBlock> 
	{
		private static final long serialVersionUID = -5270171193018691692L;
		
		private IndexRange _ixrange;
		private int _brlen; 
		private int _bclen;
		
		public SliceBlock(IndexRange ixrange, MatrixCharacteristics mcOut) {
			_ixrange = ixrange;
			_brlen = OptimizerUtils.getDefaultFrameSize();
			_bclen = (int) mcOut.getCols();
		}

		@Override
		public Iterable<Tuple2<Long, FrameBlock>> call(Tuple2<Long, FrameBlock> kv) 
			throws Exception 
		{	
			Pair<Long, FrameBlock> in = SparkUtils.toIndexedFrameBlock(kv);
			
			ArrayList<Pair<Long, FrameBlock>> outlist = new ArrayList<Pair<Long, FrameBlock>>();
			OperationsOnMatrixValues.performSlice(in, _ixrange, _brlen, _bclen, outlist);
			
			return SparkUtils.fromIndexedFrameBlock(outlist);
		}		
	}
}
