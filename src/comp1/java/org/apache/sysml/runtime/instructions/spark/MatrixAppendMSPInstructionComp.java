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

import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;

public class MatrixAppendMSPInstructionComp
{
	/**
	 * 
	 */
	/*private*/ static class MapSideAppendFunction implements  PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 2738541014432173450L;
		
		private PartitionedBroadcast<MatrixBlock> _pm = null;
		private boolean _cbind = true;
		private long _offset; 
		private int _brlen; 
		private int _bclen;
		private long _lastBlockColIndex;
		
		public MapSideAppendFunction(PartitionedBroadcast<MatrixBlock> binput, boolean cbind, long offset, int brlen, int bclen)  
		{
			_pm = binput;
			_cbind = cbind;
			
			_offset = offset;
			_brlen = brlen;
			_bclen = bclen;
			
			//check for boundary block
			int blen = cbind ? bclen : brlen;
			_lastBlockColIndex = (long)Math.ceil((double)_offset/blen);			
		}
		
		@Override
		public Iterable<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> kv) 
			throws Exception 
		{
			ArrayList<Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes, MatrixBlock>>();
			
			IndexedMatrixValue in1 = SparkUtils.toIndexedMatrixBlock(kv);
			MatrixIndexes ix = in1.getIndexes();
			
			//case 1: pass through of non-boundary blocks
			if( (_cbind?ix.getColumnIndex():ix.getRowIndex())!=_lastBlockColIndex ) 
			{
				ret.add( kv );
			}
			//case 2: pass through full input block and rhs block 
			else if( _cbind && in1.getValue().getNumColumns() == _bclen 
					|| !_cbind && in1.getValue().getNumRows() == _brlen) 
			{				
				//output lhs block
				ret.add( kv );
				
				//output shallow copy of rhs block
				if( _cbind ) {
					ret.add( new Tuple2<MatrixIndexes, MatrixBlock>(
							new MatrixIndexes(ix.getRowIndex(), ix.getColumnIndex()+1),
							_pm.getBlock((int)ix.getRowIndex(), 1)) );
				}
				else { //rbind
					ret.add( new Tuple2<MatrixIndexes, MatrixBlock>(
							new MatrixIndexes(ix.getRowIndex()+1, ix.getColumnIndex()),
							_pm.getBlock(1, (int)ix.getColumnIndex())) );	
				}
			}
			//case 3: append operation on boundary block
			else 
			{
				//allocate space for the output value
				ArrayList<IndexedMatrixValue> outlist=new ArrayList<IndexedMatrixValue>(2);
				IndexedMatrixValue first = new IndexedMatrixValue(new MatrixIndexes(ix), new MatrixBlock());
				outlist.add(first);
				
				MatrixBlock value_in2 = null;
				if( _cbind ) {
					value_in2 = _pm.getBlock((int)ix.getRowIndex(), 1);
					if(in1.getValue().getNumColumns()+value_in2.getNumColumns()>_bclen) {
						IndexedMatrixValue second=new IndexedMatrixValue(new MatrixIndexes(), new MatrixBlock());
						second.getIndexes().setIndexes(ix.getRowIndex(), ix.getColumnIndex()+1);
						outlist.add(second);
					}
				}
				else { //rbind
					value_in2 = _pm.getBlock(1, (int)ix.getColumnIndex());
					if(in1.getValue().getNumRows()+value_in2.getNumRows()>_brlen) {
						IndexedMatrixValue second=new IndexedMatrixValue(new MatrixIndexes(), new MatrixBlock());
						second.getIndexes().setIndexes(ix.getRowIndex()+1, ix.getColumnIndex());
						outlist.add(second);
					}
				}
	
				OperationsOnMatrixValues.performAppend(in1.getValue(), value_in2, outlist, _brlen, _bclen, _cbind, true, 0);	
				ret.addAll(SparkUtils.fromIndexedMatrixBlock(outlist));
			}
			
			return ret;
		}
	}
}
