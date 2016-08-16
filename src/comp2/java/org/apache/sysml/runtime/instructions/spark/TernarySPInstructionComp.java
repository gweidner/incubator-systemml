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

import org.apache.sysml.runtime.functionobjects.CTable;
import org.apache.sysml.runtime.matrix.data.CTableMap;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.Pair;
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
		public Iterator<Tuple2<MatrixIndexes, Double>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) 
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
			
			return retVal.iterator();
		}
	}
	
	/*private*/ static class ExtractBinaryCellsFromCTable implements PairFlatMapFunction<CTableMap, MatrixIndexes, Double> {

		private static final long serialVersionUID = -5933677686766674444L;
		
		@SuppressWarnings("deprecation")
		@Override
		public Iterator<Tuple2<MatrixIndexes, Double>> call(CTableMap ctableMap)
				throws Exception {
			ArrayList<Tuple2<MatrixIndexes, Double>> retVal = new ArrayList<Tuple2<MatrixIndexes, Double>>();
			
			for(LLDoubleEntry ijv : ctableMap.entrySet()) {
				long i = ijv.key1;
				long j =  ijv.key2;
				double v =  ijv.value;
				
				// retVal.add(new Tuple2<MatrixIndexes, MatrixCell>(blockIndexes, cell));
				retVal.add(new Tuple2<MatrixIndexes, Double>(new MatrixIndexes(i, j), v));
			}
			return retVal.iterator();
		}
		
	}
}
