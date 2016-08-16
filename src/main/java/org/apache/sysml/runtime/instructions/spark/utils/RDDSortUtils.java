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

package org.apache.sysml.runtime.instructions.spark.utils;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysml.runtime.functionobjects.SortIndex;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBlock;
import org.apache.sysml.runtime.instructions.spark.data.RowMatrixBlock;
import org.apache.sysml.runtime.instructions.spark.functions.ReplicateVectorFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtilsComp.ConvertToBinaryBlockFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtilsComp.ConvertToBinaryBlockFunction2;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtilsComp.ConvertToBinaryBlockFunction3;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtilsComp.ConvertToBinaryBlockFunction4;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtilsComp.ExtractDoubleValuesFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtilsComp.ExtractDoubleValuesFunction2;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtilsComp.ExtractDoubleValuesWithIndexFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtilsComp.ShuffleMatrixBlockRowsFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtilsComp.ShuffleMatrixBlockRowsInMemFunction;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;

/**
 * 
 */
public class RDDSortUtils 
{
	
	/**
	 * 
	 * @param in
	 * @param rlen
	 * @param brlen
	 * @return
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> sortByVal( JavaPairRDD<MatrixIndexes, MatrixBlock> in, long rlen, int brlen )
	{
		//create value-index rdd from inputs
		JavaRDD<Double> dvals = in.values()
				.flatMap(new ExtractDoubleValuesFunction());
	
		//sort (creates sorted range per partition)
		long hdfsBlocksize = InfrastructureAnalyzer.getHDFSBlockSize();
		int numPartitions = (int)Math.ceil(((double)rlen*8)/hdfsBlocksize);
		JavaRDD<Double> sdvals = dvals
				.sortBy(new CreateDoubleKeyFunction(), true, numPartitions);
		
		//create binary block output
		JavaPairRDD<MatrixIndexes, MatrixBlock> ret = sdvals
				.zipWithIndex()
		        .mapPartitionsToPair(new ConvertToBinaryBlockFunction(rlen, brlen));
		ret = RDDAggregateUtils.mergeByKey(ret);	
		
		return ret;
	}
	
	/**
	 * 
	 * @param in
	 * @param in2
	 * @param rlen
	 * @param brlen
	 * @return
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> sortByVal( JavaPairRDD<MatrixIndexes, MatrixBlock> in, 
			JavaPairRDD<MatrixIndexes, MatrixBlock> in2, long rlen, int brlen )
	{
		//create value-index rdd from inputs
		JavaRDD<DoublePair> dvals = in.join(in2).values()
				.flatMap(new ExtractDoubleValuesFunction2());
	
		//sort (creates sorted range per partition)
		long hdfsBlocksize = InfrastructureAnalyzer.getHDFSBlockSize();
		int numPartitions = (int)Math.ceil(((double)rlen*8)/hdfsBlocksize);
		JavaRDD<DoublePair> sdvals = dvals
				.sortBy(new CreateDoubleKeyFunction2(), true, numPartitions);

		//create binary block output
		JavaPairRDD<MatrixIndexes, MatrixBlock> ret = sdvals
				.zipWithIndex()
		        .mapPartitionsToPair(new ConvertToBinaryBlockFunction2(rlen, brlen));
		ret = RDDAggregateUtils.mergeByKey(ret);		
		
		return ret;
	}
	
	/**
	 * 
	 * @param in
	 * @param rlen
	 * @param brlen
	 * @return
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> sortIndexesByVal( JavaPairRDD<MatrixIndexes, MatrixBlock> val, 
			boolean asc, long rlen, int brlen )
	{
		//create value-index rdd from inputs
		JavaPairRDD<ValueIndexPair, Double> dvals = val
				.flatMapToPair(new ExtractDoubleValuesWithIndexFunction(brlen));
	
		//sort (creates sorted range per partition)
		long hdfsBlocksize = InfrastructureAnalyzer.getHDFSBlockSize();
		int numPartitions = (int)Math.ceil(((double)rlen*16)/hdfsBlocksize);
		JavaRDD<ValueIndexPair> sdvals = dvals
				.sortByKey(new IndexComparator(asc), true, numPartitions)
				.keys(); //workaround for index comparator
	 
		//create binary block output
		JavaPairRDD<MatrixIndexes, MatrixBlock> ret = sdvals
				.zipWithIndex()
		        .mapPartitionsToPair(new ConvertToBinaryBlockFunction3(rlen, brlen));
		ret = RDDAggregateUtils.mergeByKey(ret);		
		
		return ret;	
	}
	
	/**
	 * 
	 * @param val
	 * @param data
	 * @param asc
	 * @param rlen
	 * @param brlen
	 * @return
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> sortDataByVal( JavaPairRDD<MatrixIndexes, MatrixBlock> val, 
			JavaPairRDD<MatrixIndexes, MatrixBlock> data, boolean asc, long rlen, long clen, int brlen, int bclen )
	{
		//create value-index rdd from inputs
		JavaPairRDD<ValueIndexPair, Double> dvals = val
				.flatMapToPair(new ExtractDoubleValuesWithIndexFunction(brlen));
	
		//sort (creates sorted range per partition)
		long hdfsBlocksize = InfrastructureAnalyzer.getHDFSBlockSize();
		int numPartitions = (int)Math.ceil(((double)rlen*16)/hdfsBlocksize);
		JavaRDD<ValueIndexPair> sdvals = dvals
				.sortByKey(new IndexComparator(asc), true, numPartitions)
				.keys(); //workaround for index comparator
	 
		//create target indexes by original index
		long numRep = (long)Math.ceil((double)clen/bclen);
		JavaPairRDD<MatrixIndexes, MatrixBlock> ixmap = sdvals
				.zipWithIndex()
				.mapToPair(new ExtractIndexFunction())
				.sortByKey()
		        .mapPartitionsToPair(new ConvertToBinaryBlockFunction4(rlen, brlen));
		ixmap = RDDAggregateUtils.mergeByKey(ixmap);		
		
		//replicate indexes for all column blocks
		JavaPairRDD<MatrixIndexes, MatrixBlock> rixmap = ixmap
				.flatMapToPair(new ReplicateVectorFunction(false, numRep));      
		
		//create binary block output
		JavaPairRDD<MatrixIndexes, RowMatrixBlock> ret = data
				.join(rixmap)
				.mapPartitionsToPair(new ShuffleMatrixBlockRowsFunction(rlen, brlen));
		return RDDAggregateUtils.mergeRowsByKey(ret);
	}
	
	/**
	 * This function collects and sorts value column in memory and then broadcasts it. 
	 * 
	 * @param val
	 * @param data
	 * @param asc
	 * @param rlen
	 * @param brlen
	 * @param bclen
	 * @param ec
	 * @param r_op
	 * @return
	 * @throws DMLRuntimeException 
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> sortDataByValMemSort( JavaPairRDD<MatrixIndexes, MatrixBlock> val, 
			JavaPairRDD<MatrixIndexes, MatrixBlock> data, boolean asc, long rlen, long clen, int brlen, int bclen, 
			SparkExecutionContext sec, ReorgOperator r_op) 
					throws DMLRuntimeException
	{
		//collect orderby column for in-memory sorting
		MatrixBlock inMatBlock = SparkExecutionContext
				.toMatrixBlock(val, (int)rlen, 1, brlen, bclen, -1);

		//in-memory sort operation (w/ index return: source index in target position)
		ReorgOperator lrop = new ReorgOperator(SortIndex.getSortIndexFnObject(1, !asc, true));	
		MatrixBlock sortedIx = (MatrixBlock) inMatBlock
				.reorgOperations(lrop, new MatrixBlock(), -1, -1, -1);
		
		//flip sort indices from <source ix in target pos> to <target ix in source pos>
		MatrixBlock sortedIxSrc = new MatrixBlock(sortedIx.getNumRows(), 1, false); 
		for (int i=0; i < sortedIx.getNumRows(); i++) 
			sortedIxSrc.quickSetValue((int)sortedIx.quickGetValue(i,0)-1, 0, i+1);			

		//broadcast index vector
		PartitionedBlock<MatrixBlock> pmb = new PartitionedBlock<MatrixBlock>(sortedIxSrc, brlen, bclen);		
		Broadcast<PartitionedBlock<MatrixBlock>> _pmb = sec.getSparkContext().broadcast(pmb);	

		//sort data with broadcast index vector
		JavaPairRDD<MatrixIndexes, RowMatrixBlock> ret = data
				.mapPartitionsToPair(new ShuffleMatrixBlockRowsInMemFunction(rlen, brlen, _pmb));
		return RDDAggregateUtils.mergeRowsByKey(ret);
	}
	
	/**
	 * 
	 */
	private static class CreateDoubleKeyFunction implements Function<Double,Double> 
	{
		private static final long serialVersionUID = 2021786334763247835L;

		@Override
		public Double call(Double arg0) 
			throws Exception 
		{
			return arg0;
		}		
	}
	
	/**
	 * 
	 */
	private static class CreateDoubleKeyFunction2 implements Function<DoublePair,Double> 
	{
		private static final long serialVersionUID = -7954819651274239592L;

		@Override
		public Double call(DoublePair arg0) 
			throws Exception 
		{
			return arg0.val1;
		}		
	}
	
	/**
	 * 
	 */
	private static class ExtractIndexFunction implements PairFunction<Tuple2<ValueIndexPair,Long>,Long,Long> 
	{
		private static final long serialVersionUID = -4553468724131249535L;

		@Override
		public Tuple2<Long, Long> call(Tuple2<ValueIndexPair,Long> arg0)
				throws Exception 
		{
			return new Tuple2<Long,Long>(arg0._1().ix, arg0._2());
		}

	}
	
	/**
	 * More memory-efficient representation than Tuple2<Double,Double> which requires
	 * three instead of one object per cell.
	 */
	/*private*/ static class DoublePair implements Serializable
	{
		private static final long serialVersionUID = 4373356163734559009L;
		
		public double val1;
		public double val2;
		
		public DoublePair(double d1, double d2) {
			val1 = d1;
			val2 = d2;
		}
	}
	
	/**
	 * 
	 */
	/*private*/ static class ValueIndexPair implements Serializable 
	{
		private static final long serialVersionUID = -3273385845538526829L;
		
		public double val; 
		public long ix; 

		public ValueIndexPair(double dval, long lix) {
			val = dval;
			ix = lix;
		}
	}
	
	public static class IndexComparator implements Comparator<ValueIndexPair>, Serializable 
	{
		private static final long serialVersionUID = 5154839870549241343L;
		
		private boolean _asc;
		public IndexComparator(boolean asc) {
			_asc = asc;
		}
			
		@Override
		public int compare(ValueIndexPair o1, ValueIndexPair o2) 
		{
			//note: use conversion to Double and Long instead of native
			//compare for compatibility with jdk 6
			int retVal = Double.valueOf(o1.val).compareTo(o2.val);
			if(retVal != 0) {
				return (_asc ? retVal : -1*retVal);
			}
			else {
				//for stable sort
				return Long.valueOf(o1.ix).compareTo(o2.ix);
			}
		}
		
	}
}
