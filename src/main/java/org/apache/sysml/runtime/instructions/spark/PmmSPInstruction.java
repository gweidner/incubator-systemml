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


import org.apache.spark.api.java.JavaPairRDD;

import org.apache.sysml.lops.MapMult.CacheType;
import org.apache.sysml.lops.PMMJ;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.PmmSPInstructionComp.RDDPMMFunction;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;

/**
 * 
 */
public class PmmSPInstruction extends BinarySPInstruction 
{
	
	private CacheType _type = null;
	private CPOperand _nrow = null;
	
	public PmmSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, CPOperand nrow,
			                CacheType type, String opcode, String istr )
	{
		super(op, in1, in2, out, opcode, istr);
		_sptype = SPINSTRUCTION_TYPE.PMM;		
		_type = type;
		_nrow = nrow;
	}

	/**
	 * 
	 * @param str
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static PmmSPInstruction parseInstruction( String str ) 
		throws DMLRuntimeException 
	{
		String parts[] = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = InstructionUtils.getOpCode(str);

		if ( opcode.equalsIgnoreCase(PMMJ.OPCODE)) {
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			CPOperand nrow = new CPOperand(parts[3]);
			CPOperand out = new CPOperand(parts[4]);
			CacheType type = CacheType.valueOf(parts[5]);
			
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
			return new PmmSPInstruction(aggbin, in1, in2, out, nrow, type, opcode, str);
		} 
		else {
			throw new DMLRuntimeException("PmmSPInstruction.parseInstruction():: Unknown opcode " + opcode);
		}	
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{	
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		
		String rddVar = (_type==CacheType.LEFT) ? input2.getName() : input1.getName();
		String bcastVar = (_type==CacheType.LEFT) ? input1.getName() : input2.getName();
		MatrixCharacteristics mc = sec.getMatrixCharacteristics(output.getName());
		long rlen = sec.getScalarInput(_nrow.getName(), _nrow.getValueType(), _nrow.isLiteral()).getLongValue();
		
		//get inputs
		JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryBlockRDDHandleForVariable( rddVar );
		PartitionedBroadcast<MatrixBlock> in2 = sec.getBroadcastForVariable( bcastVar ); 
		
		//execute pmm instruction
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = in1
				.flatMapToPair( new RDDPMMFunction(_type, in2, rlen, mc.getRowsPerBlock()) );
		out = RDDAggregateUtils.sumByKeyStable(out);
		
		//put output RDD handle into symbol table
		sec.setRDDHandleForVariable(output.getName(), out);
		sec.addLineageRDD(output.getName(), rddVar);
		sec.addLineageBroadcast(output.getName(), bcastVar);
		
		//update output statistics if not inferred
		updateBinaryMMOutputMatrixCharacteristics(sec, false);
	}
}
