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

import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject.UpdateType;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.functionobjects.OffsetColumnIndex;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.AppendGSPInstructionComp.ShiftMatrix;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;

public class AppendGSPInstruction extends BinarySPInstruction
{
	private boolean _cbind = true;
	
	public AppendGSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand offset, CPOperand offset2, CPOperand out, boolean cbind, String opcode, String istr)
	{
		super(op, in1, in2, out, opcode, istr);
		_sptype = SPINSTRUCTION_TYPE.GAppend;
		_cbind = cbind;
	}
	
	public static AppendGSPInstruction parseInstruction ( String str ) 
		throws DMLRuntimeException 
	{	
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 6);
		
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand in3 = new CPOperand(parts[3]);
		CPOperand in4 = new CPOperand(parts[4]);
		CPOperand out = new CPOperand(parts[5]);
		boolean cbind = Boolean.parseBoolean(parts[6]);
		
		if(!opcode.equalsIgnoreCase("gappend"))
			throw new DMLRuntimeException("Unknown opcode while parsing a AppendGSPInstruction: " + str);
		
		return new AppendGSPInstruction(
				new ReorgOperator(OffsetColumnIndex.getOffsetColumnIndexFnObject(-1)), 
				in1, in2, in3, in4, out, cbind, opcode, str);
	}
	
	@Override
	public void processInstruction(ExecutionContext ec)
		throws DMLRuntimeException 
	{
		// general case append (map-extend, aggregate)
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		checkBinaryAppendInputCharacteristics(sec, _cbind, false, false);
		MatrixCharacteristics mc1 = sec.getMatrixCharacteristics(input1.getName());
		MatrixCharacteristics mc2 = sec.getMatrixCharacteristics(input2.getName());
		
		JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryBlockRDDHandleForVariable( input1.getName() );
		JavaPairRDD<MatrixIndexes,MatrixBlock> in2 = sec.getBinaryBlockRDDHandleForVariable( input2.getName() );
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = null;
		
		// General case: This one needs shifting and merging and hence has huge performance hit.
		JavaPairRDD<MatrixIndexes,MatrixBlock> shifted_in2 = in2
				.flatMapToPair(new ShiftMatrix(mc1, mc2, _cbind));
		out = in1.cogroup(shifted_in2)
				.mapToPair(new MergeWithShiftedBlocks(mc1, mc2, _cbind));
		
		//put output RDD handle into symbol table
		updateBinaryAppendOutputMatrixCharacteristics(sec, _cbind);
		sec.setRDDHandleForVariable(output.getName(), out);
		sec.addLineageRDD(output.getName(), input1.getName());
		sec.addLineageRDD(output.getName(), input2.getName());
	}
	
	/**
	 * 
	 */
	public static class MergeWithShiftedBlocks implements PairFunction<Tuple2<MatrixIndexes,Tuple2<Iterable<MatrixBlock>,Iterable<MatrixBlock>>>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 848955582909209400L;
		
		private boolean _cbind;
		private long _lastIxLeft;
		private int _blen;
		
		public MergeWithShiftedBlocks(MatrixCharacteristics mc1, MatrixCharacteristics mc2, boolean cbind) 
		{
			_cbind = cbind;
			_blen = cbind ? mc1.getColsPerBlock() : mc1.getRowsPerBlock();
			_lastIxLeft = (long) Math.ceil((double)(cbind ? mc1.getCols():mc1.getRows()) / _blen);			
		}

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<MatrixIndexes, Tuple2<Iterable<MatrixBlock>, Iterable<MatrixBlock>>> kv)
				throws Exception 
		{
			Iterator<MatrixBlock> iterLeft = kv._2._1.iterator();
			Iterator<MatrixBlock> iterRight = kv._2._2.iterator();
			
			//handle single left/right block input
			if( !iterLeft.hasNext() ) {
				MatrixBlock tmp = iterRight.next();
				if( iterRight.hasNext() )
					tmp.merge(iterRight.next(), false);
				return new Tuple2<MatrixIndexes, MatrixBlock>(kv._1, tmp);
			}
			else if ( !iterRight.hasNext() ) {	
				return new Tuple2<MatrixIndexes, MatrixBlock>(kv._1, iterLeft.next());
			}
			
			MatrixBlock firstBlk = iterLeft.next();
			MatrixBlock secondBlk = iterRight.next();
			long ix = _cbind ? kv._1.getColumnIndex() : kv._1.getRowIndex();
			
			// Since merge requires the dimensions matching
			if( ix == _lastIxLeft && (_cbind && firstBlk.getNumColumns() < secondBlk.getNumColumns()
				 || !_cbind && firstBlk.getNumRows()<secondBlk.getNumRows()) ) 
			{
				// This case occurs for last block of LHS matrix
				MatrixBlock tmp = new MatrixBlock(secondBlk.getNumRows(), secondBlk.getNumColumns(), true);
				firstBlk = tmp.leftIndexingOperations(firstBlk, 0, firstBlk.getNumRows()-1, 0, firstBlk.getNumColumns()-1, new MatrixBlock(), UpdateType.INPLACE_PINNED);
			}
			
			//merge with sort since blocks might be in any order
			firstBlk.merge(secondBlk, false);
			return new Tuple2<MatrixIndexes, MatrixBlock>(kv._1, firstBlk);
		}
		
	}
}