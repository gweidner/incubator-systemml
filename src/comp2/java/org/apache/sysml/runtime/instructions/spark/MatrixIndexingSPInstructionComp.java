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

import org.apache.sysml.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.util.IndexRange;

public class MatrixIndexingSPInstructionComp
{	
	/**
	 * 
	 */
	/*private*/ static class SliceRHSForLeftIndexing implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>, MatrixIndexes, MatrixBlock> 
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
			_brlen = mcLeft.getRowsPerBlock();
			_bclen = mcLeft.getColsPerBlock();
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> rightKV) 
			throws Exception 
		{
			IndexedMatrixValue in = SparkUtils.toIndexedMatrixBlock(rightKV);			
			ArrayList<IndexedMatrixValue> out = new ArrayList<IndexedMatrixValue>();
			OperationsOnMatrixValues.performShift(in, _ixrange, _brlen, _bclen, _rlen, _clen, out);
			return SparkUtils.fromIndexedMatrixBlock(out).iterator();
		}		
	}

	/**
	 * 
	 */
	/*private*/ static class SliceBlock implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 5733886476413136826L;
		
		private IndexRange _ixrange;
		private int _brlen; 
		private int _bclen;
		
		public SliceBlock(IndexRange ixrange, MatrixCharacteristics mcOut) {
			_ixrange = ixrange;
			_brlen = mcOut.getRowsPerBlock();
			_bclen = mcOut.getColsPerBlock();
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> kv) 
			throws Exception 
		{	
			IndexedMatrixValue in = SparkUtils.toIndexedMatrixBlock(kv);
			
			ArrayList<IndexedMatrixValue> outlist = new ArrayList<IndexedMatrixValue>();
			OperationsOnMatrixValues.performSlice(in, _ixrange, _brlen, _bclen, outlist);
			
			return SparkUtils.fromIndexedMatrixBlock(outlist).iterator();
		}		
	}
}
