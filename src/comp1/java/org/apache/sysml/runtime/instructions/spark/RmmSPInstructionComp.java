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


import java.util.LinkedList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.TripleIndexes;

/**
 * 
 */
public class RmmSPInstructionComp
{
	/**
	 * 
	 */
	/*private*/ static class RmmReplicateFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, TripleIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 3577072668341033932L;
		
		private long _len = -1;
		private long _blen = -1;
		private boolean _left = false;
		
		public RmmReplicateFunction(long len, long blen, boolean left)
		{
			_len = len;
			_blen = blen;
			_left = left;
		}
		
		@Override
		public Iterable<Tuple2<TripleIndexes, MatrixBlock>> call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			LinkedList<Tuple2<TripleIndexes, MatrixBlock>> ret = new LinkedList<Tuple2<TripleIndexes, MatrixBlock>>();
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();
			
			long numBlocks = (long) Math.ceil((double)_len/_blen); 
			
			if( _left ) //LHS MATRIX
			{
				//replicate wrt # column blocks in RHS
				long i = ixIn.getRowIndex();
				long k = ixIn.getColumnIndex();
				for( long j=1; j<=numBlocks; j++ ) {
					TripleIndexes tmptix = new TripleIndexes(i, j, k);
					MatrixBlock tmpblk = new MatrixBlock(blkIn);
					ret.add( new Tuple2<TripleIndexes, MatrixBlock>(tmptix, tmpblk) );
				}
			} 
			else // RHS MATRIX
			{
				//replicate wrt # row blocks in LHS
				long k = ixIn.getRowIndex();
				long j = ixIn.getColumnIndex();
				for( long i=1; i<=numBlocks; i++ ) {
					TripleIndexes tmptix = new TripleIndexes(i, j, k);
					MatrixBlock tmpblk = new MatrixBlock(blkIn);
					ret.add( new Tuple2<TripleIndexes, MatrixBlock>(tmptix, tmpblk) );
				}
			}
			
			//output list of new tuples
			return ret;
		}
	}
}
