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

import org.apache.sysml.lops.MapMult.CacheType;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;

/**
 * TODO: we need to reason about multiple broadcast variables for chains of mapmults (sum of operations until cleanup) 
 * 
 */
public class MapmmSPInstructionComp
{
	/**
	 * 
	 * 
	 */
	/*private*/ static class RDDFlatMapMMFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = -6076256569118957281L;
		
		private CacheType _type = null;
		private AggregateBinaryOperator _op = null;
		private PartitionedBroadcast<MatrixBlock> _pbc = null;
		
		public RDDFlatMapMMFunction( CacheType type, PartitionedBroadcast<MatrixBlock> binput )
		{
			_type = type;
			_pbc = binput;
			
			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}
		
		@Override
		public Iterable<Tuple2<MatrixIndexes, MatrixBlock>> call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			ArrayList<Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes, MatrixBlock>>();
			
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();

			if( _type == CacheType.LEFT )
			{
				//for all matching left-hand-side blocks
				int len = _pbc.getNumRowBlocks();
				for( int i=1; i<=len; i++ ) 
				{
					MatrixBlock left = _pbc.getBlock(i, (int)ixIn.getRowIndex());
					MatrixIndexes ixOut = new MatrixIndexes();
					MatrixBlock blkOut = new MatrixBlock();
					
					//execute matrix-vector mult
					OperationsOnMatrixValues.performAggregateBinary( 
							new MatrixIndexes(i,ixIn.getRowIndex()), left, ixIn, blkIn, ixOut, blkOut, _op);	
					
					ret.add(new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, blkOut));
				}
			}
			else //if( _type == CacheType.RIGHT )
			{
				//for all matching right-hand-side blocks
				int len = _pbc.getNumColumnBlocks();
				for( int j=1; j<=len; j++ ) 
				{
					//get the right hand side matrix
					MatrixBlock right = _pbc.getBlock((int)ixIn.getColumnIndex(), j);
					MatrixIndexes ixOut = new MatrixIndexes();
					MatrixBlock blkOut = new MatrixBlock();
					
					//execute matrix-vector mult
					OperationsOnMatrixValues.performAggregateBinary(
							ixIn, blkIn, new MatrixIndexes(ixIn.getColumnIndex(),j), right, ixOut, blkOut, _op);					
				
					ret.add(new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, blkOut));
				}
			}
			
			return ret;
		}
	}
}
