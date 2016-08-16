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
import org.apache.sysml.runtime.matrix.data.LibMatrixReorg;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.operators.Operator;


public class ParameterizedBuiltinSPInstructionComp
{	
	/**
	 * 
	 */
	public static class RDDRemoveEmptyFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes,Tuple2<MatrixBlock, MatrixBlock>>,MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = 4906304771183325289L;

		private boolean _rmRows; 
		private long _len;
		private long _brlen;
		private long _bclen;
				
		public RDDRemoveEmptyFunction(boolean rmRows, long len, long brlen, long bclen) 
		{
			_rmRows = rmRows;
			_len = len;
			_brlen = brlen;
			_bclen = bclen;
		}

		@Override
		public Iterable<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, Tuple2<MatrixBlock, MatrixBlock>> arg0)
			throws Exception 
		{
			//prepare inputs (for internal api compatibility)
			IndexedMatrixValue data = SparkUtils.toIndexedMatrixBlock(arg0._1(),arg0._2()._1());
			IndexedMatrixValue offsets = SparkUtils.toIndexedMatrixBlock(arg0._1(),arg0._2()._2());
			
			//execute remove empty operations
			ArrayList<IndexedMatrixValue> out = new ArrayList<IndexedMatrixValue>();
			LibMatrixReorg.rmempty(data, offsets, _rmRows, _len, _brlen, _bclen, out);

			//prepare and return outputs
			return SparkUtils.fromIndexedMatrixBlock(out);
		}
	}
	
	/**
	 * 
	 */
	public static class RDDRemoveEmptyFunctionInMem implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>,MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = 4906304771183325289L;

		private boolean _rmRows; 
		private long _len;
		private long _brlen;
		private long _bclen;
		
		private PartitionedBroadcast<MatrixBlock> _off = null;
				
		public RDDRemoveEmptyFunctionInMem(boolean rmRows, long len, long brlen, long bclen, PartitionedBroadcast<MatrixBlock> off) 
		{
			_rmRows = rmRows;
			_len = len;
			_brlen = brlen;
			_bclen = bclen;
			_off = off;
		}

		@Override
		public Iterable<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
			throws Exception 
		{
			//prepare inputs (for internal api compatibility)
			IndexedMatrixValue data = SparkUtils.toIndexedMatrixBlock(arg0._1(),arg0._2());
			//IndexedMatrixValue offsets = SparkUtils.toIndexedMatrixBlock(arg0._1(),arg0._2()._2());
			IndexedMatrixValue offsets = null;
			if(_rmRows)
				offsets = SparkUtils.toIndexedMatrixBlock(arg0._1(), _off.getBlock((int)arg0._1().getRowIndex(), 1));
			else
				offsets = SparkUtils.toIndexedMatrixBlock(arg0._1(), _off.getBlock(1, (int)arg0._1().getColumnIndex()));
			
			//execute remove empty operations
			ArrayList<IndexedMatrixValue> out = new ArrayList<IndexedMatrixValue>();
			LibMatrixReorg.rmempty(data, offsets, _rmRows, _len, _brlen, _bclen, out);

			//prepare and return outputs
			return SparkUtils.fromIndexedMatrixBlock(out);
		}
	}
	
	/**
	 * 
	 */
	public static class RDDRExpandFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>,MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = -6153643261956222601L;
		
		private double _maxVal;
		private boolean _dirRows;
		private boolean _cast;
		private boolean _ignore;
		private long _brlen;
		private long _bclen;
		
		public RDDRExpandFunction(double maxVal, boolean dirRows, boolean cast, boolean ignore, long brlen, long bclen) 
		{
			_maxVal = maxVal;
			_dirRows = dirRows;
			_cast = cast;
			_ignore = ignore;
			_brlen = brlen;
			_bclen = bclen;
		}

		@Override
		public Iterable<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
			throws Exception 
		{
			//prepare inputs (for internal api compatibility)
			IndexedMatrixValue data = SparkUtils.toIndexedMatrixBlock(arg0._1(),arg0._2());
			
			//execute rexpand operations
			ArrayList<IndexedMatrixValue> out = new ArrayList<IndexedMatrixValue>();
			LibMatrixReorg.rexpand(data, _maxVal, _dirRows, _cast, _ignore, _brlen, _bclen, out);
			
			//prepare and return outputs
			return SparkUtils.fromIndexedMatrixBlock(out);
		}
	}
	
	public static class RDDMapGroupedAggFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>,MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = 6795402640178679851L;
		
		private PartitionedBroadcast<MatrixBlock> _pbm = null;
		private Operator _op = null;
		private int _ngroups = -1;
		private int _brlen = -1;
		private int _bclen = -1;
		
		public RDDMapGroupedAggFunction(PartitionedBroadcast<MatrixBlock> pbm, Operator op, int ngroups, int brlen, int bclen) 
		{
			_pbm = pbm;
			_op = op;
			_ngroups = ngroups;
			_brlen = brlen;
			_bclen = bclen;
		}

		@Override
		public Iterable<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
			throws Exception 
		{
			//get all inputs
			MatrixIndexes ix = arg0._1();
			MatrixBlock target = arg0._2();		
			MatrixBlock groups = _pbm.getBlock((int)ix.getRowIndex(), 1);
			
			//execute map grouped aggregate operations
			IndexedMatrixValue in1 = SparkUtils.toIndexedMatrixBlock(ix, target);
			ArrayList<IndexedMatrixValue> outlist = new ArrayList<IndexedMatrixValue>();
			OperationsOnMatrixValues.performMapGroupedAggregate(_op, in1, groups, _ngroups, _brlen, _bclen, outlist);
			
			//output all result blocks
			return SparkUtils.fromIndexedMatrixBlock(outlist);
		}
	}
}
