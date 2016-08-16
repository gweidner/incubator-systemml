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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.sysml.lops.Ternary;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.functionobjects.CTable;
import org.apache.sysml.runtime.instructions.Instruction;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.CTableMap;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixCell;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.data.Pair;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.SimpleOperator;
import org.apache.sysml.runtime.util.LongLongDoubleHashMap.LLDoubleEntry;
import org.apache.sysml.runtime.util.UtilFunctions;

public class TernarySPInstructionComp
{
	/**
	 *
	 */
	/*private*/ static class ExpandScalarCtableOperation implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>, MatrixIndexes, Double> 
	{
		private static final long serialVersionUID = -12552669148928288L;
	
		private int _brlen;
		
		public ExpandScalarCtableOperation(int brlen) {
			_brlen = brlen;
		}

		@Override
		public Iterable<Tuple2<MatrixIndexes, Double>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) 
			throws Exception 
		{
			MatrixIndexes ix = arg0._1();
			MatrixBlock mb = arg0._2(); //col-vector
			
			//create an output cell per matrix block row (aligned w/ original source position)
			ArrayList<Tuple2<MatrixIndexes, Double>> retVal = new ArrayList<Tuple2<MatrixIndexes,Double>>();
			CTable ctab = CTable.getCTableFnObject();
			for( int i=0; i<mb.getNumRows(); i++ )
			{
				//compute global target indexes (via ctable obj for error handling consistency)
				long row = UtilFunctions.computeCellIndex(ix.getRowIndex(), _brlen, i);
				double v2 = mb.quickGetValue(i, 0);
				Pair<MatrixIndexes,Double> p = ctab.execute(row, v2, 1.0);
				
				//indirect construction over pair to avoid tuple2 dependency in general ctable obj
				if( p.getKey().getRowIndex() >= 1 ) //filter rejected entries
					retVal.add(new Tuple2<MatrixIndexes,Double>(p.getKey(), p.getValue()));
			}
			
			return retVal;
		}
	}
	
	/*private*/ static class ExtractBinaryCellsFromCTable implements PairFlatMapFunction<CTableMap, MatrixIndexes, Double> {

		private static final long serialVersionUID = -5933677686766674444L;
		
		@SuppressWarnings("deprecation")
		@Override
		public Iterable<Tuple2<MatrixIndexes, Double>> call(CTableMap ctableMap)
				throws Exception {
			ArrayList<Tuple2<MatrixIndexes, Double>> retVal = new ArrayList<Tuple2<MatrixIndexes, Double>>();
			
			for(LLDoubleEntry ijv : ctableMap.entrySet()) {
				long i = ijv.key1;
				long j =  ijv.key2;
				double v =  ijv.value;
				
				// retVal.add(new Tuple2<MatrixIndexes, MatrixCell>(blockIndexes, cell));
				retVal.add(new Tuple2<MatrixIndexes, Double>(new MatrixIndexes(i, j), v));
			}
			return retVal;
		}
		
	}
}
