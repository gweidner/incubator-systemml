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

import org.apache.sysml.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.LibMatrixReorg;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;

/**
 * 
 * 
 */
public class MatrixReshapeSPInstructionComp
{	
	/*private*/ static class RDDReshapeFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 2819309412002224478L;
		
		private MatrixCharacteristics _mcIn = null;
		private MatrixCharacteristics _mcOut = null;
		private boolean _byrow = true;
		
		public RDDReshapeFunction( MatrixCharacteristics mcIn, MatrixCharacteristics mcOut, boolean byrow)
		{
			_mcIn = mcIn;
			_mcOut = mcOut;
			_byrow = byrow;
		}
		
		@Override
		public Iterable<Tuple2<MatrixIndexes, MatrixBlock>> call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			//input conversion (for libmatrixreorg compatibility)
			IndexedMatrixValue in = SparkUtils.toIndexedMatrixBlock(arg0);
			
			//execute actual reshape operation
			ArrayList<IndexedMatrixValue> out = new ArrayList<IndexedMatrixValue>();			
			out = LibMatrixReorg.reshape(in, _mcIn.getRows(), _mcIn.getCols(), _mcIn.getRowsPerBlock(), _mcIn.getRowsPerBlock(),
                    out, _mcOut.getRows(), _mcOut.getCols(), _mcOut.getRowsPerBlock(), _mcOut.getColsPerBlock(), _byrow);

			//output conversion (for compatibility w/ rdd schema)
			return SparkUtils.fromIndexedMatrixBlock(out);
		}
	}
}
