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
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBlock;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;

/**
 * This pmapmm matrix multiplication instruction is still experimental
 * not integrated in automatic operator selection yet.
 * 
 */
public class PMapmmSPInstructionComp
{
	/**
	 * 
	 * 
	 */
	/*private*/ static class PMapMMFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = -4520080421816885321L;

		private AggregateBinaryOperator _op = null;
		private Broadcast<PartitionedBlock<MatrixBlock>> _pbc = null;
		private long _offset = -1;
		
		public PMapMMFunction( Broadcast<PartitionedBlock<MatrixBlock>> binput, long offset )
		{
			_pbc = binput;
			_offset = offset;
			
			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
			throws Exception 
		{
			PartitionedBlock<MatrixBlock> pm = _pbc.value();
			
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();

			MatrixIndexes ixOut = new MatrixIndexes();
			MatrixBlock blkOut = new MatrixBlock();
			
			ArrayList<Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes, MatrixBlock>>();
			
			//get the right hand side matrix
			for( int i=1; i<=pm.getNumRowBlocks(); i++ ) {
				MatrixBlock left = pm.getBlock(i, (int)ixIn.getRowIndex());
			
				//execute matrix-vector mult
				OperationsOnMatrixValues.performAggregateBinary( 
						new MatrixIndexes(i,ixIn.getRowIndex()), left, ixIn, blkIn, ixOut, blkOut, _op);						
				
				//output new tuple
				ixOut.setIndexes(_offset+i, ixOut.getColumnIndex());
				ret.add(new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, blkOut));
			}
			
			return ret.iterator();
		}
	}
}
