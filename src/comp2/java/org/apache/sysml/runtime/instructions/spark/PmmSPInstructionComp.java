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
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.lops.MapMult.CacheType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.util.UtilFunctions;

/**
 * 
 */
public class PmmSPInstructionComp 
{
	/**
	 * 
	 * 
	 */
	/*private*/ static class RDDPMMFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = -1696560050436469140L;
		
		private PartitionedBroadcast<MatrixBlock> _pmV = null;
		private long _rlen = -1;
		private int _brlen = -1;
		
		public RDDPMMFunction( CacheType type, PartitionedBroadcast<MatrixBlock> binput, long rlen, int brlen ) 
			throws DMLRuntimeException
		{
			_brlen = brlen;
			_rlen = rlen;
			_pmV = binput;
		}
		
		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock mb2 = arg0._2();
			
			//get the right hand side matrix
			MatrixBlock mb1 = _pmV.getBlock((int)ixIn.getRowIndex(), 1);
			
			//compute target block indexes
			long minPos = UtilFunctions.toLong( mb1.minNonZero() );
			long maxPos = UtilFunctions.toLong( mb1.max() );
			long rowIX1 = (minPos-1)/_brlen+1;
			long rowIX2 = (maxPos-1)/_brlen+1;
			boolean multipleOuts = (rowIX1 != rowIX2);
			
			if( minPos >= 1 ) //at least one row selected
			{
				//output sparsity estimate
				double spmb1 = OptimizerUtils.getSparsity(mb1.getNumRows(), 1, mb1.getNonZeros());
				long estnnz = (long) (spmb1 * mb2.getNonZeros());
				boolean sparse = MatrixBlock.evalSparseFormatInMemory(_brlen, mb2.getNumColumns(), estnnz);
				
				//compute and allocate output blocks
				MatrixBlock out1 = new MatrixBlock();
				MatrixBlock out2 = multipleOuts ? new MatrixBlock() : null;
				out1.reset(_brlen, mb2.getNumColumns(), sparse);
				if( out2 != null )
					out2.reset(UtilFunctions.computeBlockSize(_rlen, rowIX2, _brlen), mb2.getNumColumns(), sparse);
				
				//compute core matrix permutation (assumes that out1 has default blocksize, 
				//hence we do a meta data correction afterwards)
				mb1.permutationMatrixMultOperations(mb2, out1, out2);
				out1.setNumRows(UtilFunctions.computeBlockSize(_rlen, rowIX1, _brlen));
				ret.add(new Tuple2<MatrixIndexes, MatrixBlock>(new MatrixIndexes(rowIX1, ixIn.getColumnIndex()), out1));
				if( out2 != null )
					ret.add(new Tuple2<MatrixIndexes, MatrixBlock>(new MatrixIndexes(rowIX2, ixIn.getColumnIndex()), out2));
			}
			
			return ret.iterator();
		}
	}
}
