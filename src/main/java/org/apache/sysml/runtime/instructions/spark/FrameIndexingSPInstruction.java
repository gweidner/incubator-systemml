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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import org.apache.sysml.hops.AggBinaryOp.SparkAggType;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.FrameIndexingSPInstructionComp.SliceBlock;
import org.apache.sysml.runtime.instructions.spark.FrameIndexingSPInstructionComp.SliceRHSForLeftIndexing;
import org.apache.sysml.runtime.instructions.spark.FrameIndexingSPInstructionComp.ZeroOutLHS;
import org.apache.sysml.runtime.instructions.spark.data.LazyIterableIterator;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.instructions.spark.functions.IsFrameBlockInRange;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.FrameBlock;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.data.Pair;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.util.IndexRange;
import org.apache.sysml.runtime.util.UtilFunctions;

public class FrameIndexingSPInstruction  extends IndexingSPInstruction
{
	
	/*
	 * This class implements the frame indexing functionality inside Spark.  
	 * Example instructions: 
	 *     rangeReIndex:mVar1:Var2:Var3:Var4:Var5:mVar6
	 *         input=mVar1, output=mVar6, 
	 *         bounds = (Var2,Var3,Var4,Var5)
	 *         rowindex_lower: Var2, rowindex_upper: Var3 
	 *         colindex_lower: Var4, colindex_upper: Var5
	 *     leftIndex:mVar1:mVar2:Var3:Var4:Var5:Var6:mVar7
	 *         triggered by "mVar1[Var3:Var4, Var5:Var6] = mVar2"
	 *         the result is stored in mVar7
	 *  
	 */
	public FrameIndexingSPInstruction(Operator op, CPOperand in, CPOperand rl, CPOperand ru, CPOperand cl, CPOperand cu, 
			                          CPOperand out, SparkAggType aggtype, String opcode, String istr)
	{
		super(op, in, rl, ru, cl, cu, out, aggtype, opcode, istr);
	}
	
	public FrameIndexingSPInstruction(Operator op, CPOperand lhsInput, CPOperand rhsInput, CPOperand rl, CPOperand ru, CPOperand cl, CPOperand cu, 
			                          CPOperand out, String opcode, String istr)
	{
		super(op, lhsInput, rhsInput, rl, ru, cl, cu, out, opcode, istr);
	}
	
	
	@Override
	public void processInstruction(ExecutionContext ec)
			throws DMLRuntimeException 
	{	
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		String opcode = getOpcode();
		
		//get indexing range
		long rl = ec.getScalarInput(rowLower.getName(), rowLower.getValueType(), rowLower.isLiteral()).getLongValue();
		long ru = ec.getScalarInput(rowUpper.getName(), rowUpper.getValueType(), rowUpper.isLiteral()).getLongValue();
		long cl = ec.getScalarInput(colLower.getName(), colLower.getValueType(), colLower.isLiteral()).getLongValue();
		long cu = ec.getScalarInput(colUpper.getName(), colUpper.getValueType(), colUpper.isLiteral()).getLongValue();
		IndexRange ixrange = new IndexRange(rl, ru, cl, cu);
		
		//right indexing
		if( opcode.equalsIgnoreCase("rangeReIndex") )
		{
			//update and check output dimensions
			MatrixCharacteristics mcIn = sec.getMatrixCharacteristics(input1.getName());
			MatrixCharacteristics mcOut = sec.getMatrixCharacteristics(output.getName());
			mcOut.set(ru-rl+1, cu-cl+1, mcIn.getRowsPerBlock(), mcIn.getColsPerBlock());
			checkValidOutputDimensions(mcOut);
			
			//execute right indexing operation (partitioning-preserving if possible)
			JavaPairRDD<Long,FrameBlock> in1 = sec.getFrameBinaryBlockRDDHandleForVariable( input1.getName() );
			JavaPairRDD<Long,FrameBlock> out = null;
			if( isPartitioningPreservingRightIndexing(mcIn, ixrange) ) {
				out = in1.mapPartitionsToPair(
						new SliceBlockPartitionFunction(ixrange, mcOut), true);
			}
			else{
				out = in1.filter(new IsFrameBlockInRange(rl, ru, mcOut))
			             .flatMapToPair(new SliceBlock(ixrange, mcOut));
				
				//aggregation if required 
				if( _aggType != SparkAggType.NONE )
					out = RDDAggregateUtils.mergeByFrameKey(out);
			}
			
			//put output RDD handle into symbol table
			sec.setRDDHandleForVariable(output.getName(), out);
			sec.addLineageRDD(output.getName(), input1.getName());
		}
		//left indexing
		else if ( opcode.equalsIgnoreCase("leftIndex") || opcode.equalsIgnoreCase("mapLeftIndex"))
		{
			JavaPairRDD<Long,FrameBlock> in1 = sec.getFrameBinaryBlockRDDHandleForVariable( input1.getName() );
			PartitionedBroadcast<FrameBlock> broadcastIn2 = null;
			JavaPairRDD<Long,FrameBlock> in2 = null;
			JavaPairRDD<Long,FrameBlock> out = null;
			
			//update and check output dimensions
			MatrixCharacteristics mcOut = sec.getMatrixCharacteristics(output.getName());
			MatrixCharacteristics mcLeft = ec.getMatrixCharacteristics(input1.getName());
			mcOut.set(mcLeft.getRows(), mcLeft.getCols(), mcLeft.getRowsPerBlock(), mcLeft.getColsPerBlock());
			checkValidOutputDimensions(mcOut);
			
			//note: always frame rhs, scalars are preprocessed via cast to 1x1 frame
			MatrixCharacteristics mcRight = ec.getMatrixCharacteristics(input2.getName());
				
			//sanity check matching index range and rhs dimensions
			if(!mcRight.dimsKnown()) {
				throw new DMLRuntimeException("The right input frame dimensions are not specified for FrameIndexingSPInstruction");
			}
			if(!(ru-rl+1 == mcRight.getRows() && cu-cl+1 == mcRight.getCols())) {
				throw new DMLRuntimeException("Invalid index range of leftindexing: ["+rl+":"+ru+","+cl+":"+cu+"] vs ["+mcRight.getRows()+"x"+mcRight.getCols()+"]." );
			}
			
			if(opcode.equalsIgnoreCase("mapLeftIndex")) 
			{
				broadcastIn2 = sec.getBroadcastForFrameVariable( input2.getName());
				
				//partitioning-preserving mappartitions (key access required for broadcast loopkup)
				out = in1.mapPartitionsToPair(
						new LeftIndexPartitionFunction(broadcastIn2, ixrange, mcOut), true);
			}
			else { //general case
				
				// zero-out lhs
				in1 = in1.flatMapToPair(new ZeroOutLHS(false, ixrange, mcLeft));
				
				// slice rhs, shift and merge with lhs
				in2 = sec.getFrameBinaryBlockRDDHandleForVariable( input2.getName() )
					    .flatMapToPair(new SliceRHSForLeftIndexing(ixrange, mcLeft));

				out = (JavaPairRDD<Long, FrameBlock>) RDDAggregateUtils.mergeByFrameKey(in1.union(in2));
			}
			
			sec.setRDDHandleForVariable(output.getName(), out);
			sec.addLineageRDD(output.getName(), input1.getName());
			if( broadcastIn2 != null)
				sec.addLineageBroadcast(output.getName(), input2.getName());
			if(in2 != null) 
				sec.addLineageRDD(output.getName(), input2.getName());
		}
		else
			throw new DMLRuntimeException("Invalid opcode (" + opcode +") encountered in FrameIndexingSPInstruction.");		
	}
		
	/**
	 * 
	 * @param mcIn
	 * @param ixrange
	 * @return
	 */
	private boolean isPartitioningPreservingRightIndexing(MatrixCharacteristics mcIn, IndexRange ixrange)
	{
		return ( mcIn.dimsKnown() &&
			(ixrange.rowStart==1 && ixrange.rowEnd==mcIn.getRows() ));   //Entire Column/s			 
	}
	
	
	/**
	 * 
	 * @param mcOut
	 * @throws DMLRuntimeException
	 */
	private static void checkValidOutputDimensions(MatrixCharacteristics mcOut) 
		throws DMLRuntimeException
	{
		if(!mcOut.dimsKnown()) {
			throw new DMLRuntimeException("FrameIndexingSPInstruction: The updated output dimensions are invalid: " + mcOut);
		}
	}
		
	/**
	 * 
	 */
	private static class LeftIndexPartitionFunction implements PairFlatMapFunction<Iterator<Tuple2<Long,FrameBlock>>, Long, FrameBlock> 
	{
		private static final long serialVersionUID = -911940376947364915L;

		private PartitionedBroadcast<FrameBlock> _binput;
		private IndexRange _ixrange = null;
		
		public LeftIndexPartitionFunction(PartitionedBroadcast<FrameBlock> binput, IndexRange ixrange, MatrixCharacteristics mc) 
		{
			_binput = binput;
			_ixrange = ixrange;
		}

		@Override
		public LazyIterableIterator<Tuple2<Long, FrameBlock>> call(Iterator<Tuple2<Long, FrameBlock>> arg0)
			throws Exception 
		{
			return new LeftIndexPartitionIterator(arg0);
		}
		
		/**
		 * 
		 */
		private class LeftIndexPartitionIterator extends LazyIterableIterator<Tuple2<Long, FrameBlock>>
		{
			public LeftIndexPartitionIterator(Iterator<Tuple2<Long, FrameBlock>> in) {
				super(in);
			}
			
			@Override
			protected Tuple2<Long, FrameBlock> computeNext(Tuple2<Long, FrameBlock> arg) 
				throws Exception 
			{
				int iNumRowsInBlock = arg._2.getNumRows();
				int iNumCols = arg._2.getNumColumns();
				if(!UtilFunctions.isInFrameBlockRange(arg._1(), iNumRowsInBlock, iNumCols, _ixrange)) {
					return arg;
				}
				
				// Calculate global index of left hand side block
				long lhs_rl = Math.max(_ixrange.rowStart, arg._1); //Math.max(_ixrange.rowStart, (arg._1-1)*iNumRowsInBlock + 1);
				long lhs_ru = Math.min(_ixrange.rowEnd, arg._1+iNumRowsInBlock-1);
				long lhs_cl = Math.max(_ixrange.colStart, 1);
				long lhs_cu = Math.min(_ixrange.colEnd, iNumCols);
				
				// Calculate global index of right hand side block
				long rhs_rl = lhs_rl - _ixrange.rowStart + 1;
				long rhs_ru = rhs_rl + (lhs_ru - lhs_rl);
				long rhs_cl = lhs_cl - _ixrange.colStart + 1;
				long rhs_cu = rhs_cl + (lhs_cu - lhs_cl);
				
				// Provide local zero-based index to leftIndexingOperations
				int lhs_lrl = (int)(lhs_rl- arg._1);
				int lhs_lru = (int)(lhs_ru- arg._1);
				int lhs_lcl = (int)lhs_cl-1;
				int lhs_lcu = (int)lhs_cu-1;

				FrameBlock ret = arg._2;
				int brlen = OptimizerUtils.DEFAULT_BLOCKSIZE;
				long rhs_rl_pb = rhs_rl;
				long rhs_ru_pb = Math.min(rhs_ru, (((rhs_rl-1)/brlen)+1)*brlen); 
				while(rhs_rl_pb <= rhs_ru_pb) {
					// Provide global zero-based index to sliceOperations, but only for one RHS partition block at a time.
					FrameBlock slicedRHSMatBlock = _binput.sliceOperations(rhs_rl_pb, rhs_ru_pb, rhs_cl, rhs_cu, new FrameBlock());
					
					// Provide local zero-based index to leftIndexingOperations
					int lhs_lrl_pb = (int) (lhs_lrl + (rhs_rl_pb - rhs_rl));
					int lhs_lru_pb = (int) (lhs_lru + (rhs_ru_pb - rhs_ru));
					ret = ret.leftIndexingOperations(slicedRHSMatBlock, lhs_lrl_pb, lhs_lru_pb, lhs_lcl, lhs_lcu, new FrameBlock());
					rhs_rl_pb = rhs_ru_pb + 1;
					rhs_ru_pb = Math.min(rhs_ru, rhs_ru_pb+brlen);
				}
				
				return new Tuple2<Long, FrameBlock>(arg._1, ret);
			}
		}
	}

	/**
	 * 
	 */
	private static class SliceBlockPartitionFunction implements PairFlatMapFunction<Iterator<Tuple2<Long, FrameBlock>>, Long, FrameBlock> 
	{
		private static final long serialVersionUID = -1655390518299307588L;
		
		private IndexRange _ixrange;
		private int _brlen; 
		private int _bclen;
		
		public SliceBlockPartitionFunction(IndexRange ixrange, MatrixCharacteristics mcOut) {
			_ixrange = ixrange;
			_brlen = (int) Math.min(OptimizerUtils.getDefaultFrameSize(), mcOut.getRows());
			_bclen = (int) mcOut.getCols();
		}

		@Override
		public LazyIterableIterator<Tuple2<Long, FrameBlock>> call(Iterator<Tuple2<Long, FrameBlock>> arg0)
			throws Exception 
		{
			return new SliceBlockPartitionIterator(arg0);
		}	
		
		private class SliceBlockPartitionIterator extends LazyIterableIterator<Tuple2<Long, FrameBlock>>
		{
			public SliceBlockPartitionIterator(Iterator<Tuple2<Long, FrameBlock>> in) {
				super(in);
			}

			@Override
			protected Tuple2<Long, FrameBlock> computeNext(Tuple2<Long, FrameBlock> arg)
				throws Exception
			{
				Pair<Long, FrameBlock> in = SparkUtils.toIndexedFrameBlock(arg);
				
				ArrayList<Pair<Long, FrameBlock>> outlist = new ArrayList<Pair<Long, FrameBlock>>();
				OperationsOnMatrixValues.performSlice(in, _ixrange, _brlen, _bclen, outlist);
				
				assert(outlist.size() == 1); //1-1 row/column block indexing
				return SparkUtils.fromIndexedFrameBlock(outlist.get(0));
			}			
		}
	}
}
