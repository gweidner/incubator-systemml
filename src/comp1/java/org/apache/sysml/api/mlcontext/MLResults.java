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

package org.apache.sysml.api.mlcontext;

import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.controlprogram.caching.CacheException;
import org.apache.sysml.runtime.controlprogram.caching.FrameObject;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.instructions.cp.BooleanObject;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.sysml.runtime.instructions.cp.DoubleObject;
import org.apache.sysml.runtime.instructions.cp.IntObject;
import org.apache.sysml.runtime.instructions.cp.ScalarObject;
import org.apache.sysml.runtime.instructions.cp.StringObject;
import org.apache.sysml.runtime.matrix.data.FrameBlock;
import org.apache.sysml.runtime.util.DataConverter;

import scala.Tuple1;
import scala.Tuple10;
import scala.Tuple11;
import scala.Tuple12;
import scala.Tuple13;
import scala.Tuple14;
import scala.Tuple15;
import scala.Tuple16;
import scala.Tuple17;
import scala.Tuple18;
import scala.Tuple19;
import scala.Tuple2;
import scala.Tuple20;
import scala.Tuple21;
import scala.Tuple22;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple6;
import scala.Tuple7;
import scala.Tuple8;
import scala.Tuple9;

/**
 * MLResults handles the results returned from executing a Script using the
 * MLContext API.
 *
 */
public class MLResults {
	protected LocalVariableMap symbolTable = null;
	protected Script script = null;
	protected SparkExecutionContext sparkExecutionContext = null;

	public MLResults() {
	}

	public MLResults(LocalVariableMap symbolTable) {
		this.symbolTable = symbolTable;
	}

	public MLResults(Script script) {
		this.script = script;
		this.symbolTable = script.getSymbolTable();
		ScriptExecutor scriptExecutor = script.getScriptExecutor();
		ExecutionContext executionContext = scriptExecutor.getExecutionContext();
		if (executionContext instanceof SparkExecutionContext) {
			sparkExecutionContext = (SparkExecutionContext) executionContext;
		}
	}

	/**
	 * Obtain an output as a {@code Data} object.
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code Data} object
	 */
	public Data getData(String outputName) {
		Set<String> keys = symbolTable.keySet();
		if (!keys.contains(outputName)) {
			throw new MLContextException("Variable '" + outputName + "' not found");
		}
		Data data = symbolTable.get(outputName);
		return data;
	}

	/**
	 * Obtain an output as a {@code MatrixObject}
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code MatrixObject}
	 */
	public MatrixObject getMatrixObject(String outputName) {
		Data data = getData(outputName);
		if (!(data instanceof MatrixObject)) {
			throw new MLContextException("Variable '" + outputName + "' not a matrix");
		}
		MatrixObject mo = (MatrixObject) data;
		return mo;
	}

	/**
	 * Obtain an output as a two-dimensional {@code double} array.
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a two-dimensional {@code double} array
	 */
	public double[][] getDoubleMatrix(String outputName) {
		MatrixObject mo = getMatrixObject(outputName);
		double[][] doubleMatrix = MLContextConversionUtil.matrixObjectToDoubleMatrix(mo);
		return doubleMatrix;
	}

	/**
	 * Obtain an output as a {@code JavaRDD<String>} in IJV format.
	 * <p>
	 * The following matrix in DML:
	 * </p>
	 * <code>M = full('1 2 3 4', rows=2, cols=2);
	 * </code>
	 * <p>
	 * is equivalent to the following {@code JavaRDD<String>} in IJV format:
	 * </p>
	 * <code>1 1 1.0
	 * <br>1 2 2.0
	 * <br>2 1 3.0
	 * <br>2 2 4.0
	 * </code>
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code JavaRDD<String>} in IJV format
	 */
	public JavaRDD<String> getJavaRDDStringIJV(String outputName) {
		MatrixObject mo = getMatrixObject(outputName);
		JavaRDD<String> javaRDDStringIJV = MLContextConversionUtil.matrixObjectToJavaRDDStringIJV(mo);
		return javaRDDStringIJV;
	}

	/**
	 * Obtain an output as a {@code JavaRDD<String>} in CSV format.
	 * <p>
	 * The following matrix in DML:
	 * </p>
	 * <code>M = full('1 2 3 4', rows=2, cols=2);
	 * </code>
	 * <p>
	 * is equivalent to the following {@code JavaRDD<String>} in CSV format:
	 * </p>
	 * <code>1.0,2.0
	 * <br>3.0,4.0
	 * </code>
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code JavaRDD<String>} in CSV format
	 */
	public JavaRDD<String> getJavaRDDStringCSV(String outputName) {
		MatrixObject mo = getMatrixObject(outputName);
		JavaRDD<String> javaRDDStringCSV = MLContextConversionUtil.matrixObjectToJavaRDDStringCSV(mo);
		return javaRDDStringCSV;
	}

	/**
	 * Obtain an output as a {@code RDD<String>} in CSV format.
	 * <p>
	 * The following matrix in DML:
	 * </p>
	 * <code>M = full('1 2 3 4', rows=2, cols=2);
	 * </code>
	 * <p>
	 * is equivalent to the following {@code RDD<String>} in CSV format:
	 * </p>
	 * <code>1.0,2.0
	 * <br>3.0,4.0
	 * </code>
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code RDD<String>} in CSV format
	 */
	public RDD<String> getRDDStringCSV(String outputName) {
		MatrixObject mo = getMatrixObject(outputName);
		RDD<String> rddStringCSV = MLContextConversionUtil.matrixObjectToRDDStringCSV(mo);
		return rddStringCSV;
	}

	/**
	 * Obtain an output as a {@code RDD<String>} in IJV format.
	 * <p>
	 * The following matrix in DML:
	 * </p>
	 * <code>M = full('1 2 3 4', rows=2, cols=2);
	 * </code>
	 * <p>
	 * is equivalent to the following {@code RDD<String>} in IJV format:
	 * </p>
	 * <code>1 1 1.0
	 * <br>1 2 2.0
	 * <br>2 1 3.0
	 * <br>2 2 4.0
	 * </code>
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code RDD<String>} in IJV format
	 */
	public RDD<String> getRDDStringIJV(String outputName) {
		MatrixObject mo = getMatrixObject(outputName);
		RDD<String> rddStringIJV = MLContextConversionUtil.matrixObjectToRDDStringIJV(mo);
		return rddStringIJV;
	}

	/**
	 * Obtain an output as a {@code DataFrame} of doubles.
	 * <p>
	 * The following matrix in DML:
	 * </p>
	 * <code>M = full('1 2 3 4', rows=2, cols=2);
	 * </code>
	 * <p>
	 * is equivalent to the following {@code DataFrame} of doubles:
	 * </p>
	 * <code>[0.0,1.0,2.0]
	 * <br>[1.0,3.0,4.0]
	 * </code>
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code DataFrame} of doubles
	 */
	public DataFrame getDataFrame(String outputName) {
		MatrixObject mo = getMatrixObject(outputName);
		DataFrame df = MLContextConversionUtil.matrixObjectToDataFrame(mo, sparkExecutionContext, false);
		return df;
	}
	
	public DataFrame getDataFrame(String outputName, boolean isVectorDF) {
		MatrixObject mo = getMatrixObject(outputName);
		DataFrame df = MLContextConversionUtil.matrixObjectToDataFrame(mo, sparkExecutionContext, isVectorDF);
		return df;
	}

	/**
	 * Obtain an output as a {@code Matrix}.
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code Matrix}
	 */
	public Matrix getMatrix(String outputName) {
		MatrixObject mo = getMatrixObject(outputName);
		Matrix matrix = new Matrix(mo, sparkExecutionContext);
		return matrix;
	}
	

	/**
	 * Obtain an output as a {@code BinaryBlockMatrix}.
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code BinaryBlockMatrix}
	 */
	public BinaryBlockMatrix getBinaryBlockMatrix(String outputName) {
		MatrixObject mo = getMatrixObject(outputName);
		BinaryBlockMatrix binaryBlockMatrix = MLContextConversionUtil.matrixObjectToBinaryBlockMatrix(mo,
				sparkExecutionContext);
		return binaryBlockMatrix;
	}

	/**
	 * Obtain an output as a two-dimensional {@code String} array.
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a two-dimensional {@code String} array
	 */
	public String[][] getFrame(String outputName) {
		try {
			Data data = getData(outputName);
			if (!(data instanceof FrameObject)) {
				throw new MLContextException("Variable '" + outputName + "' not a frame");
			}
			FrameObject fo = (FrameObject) data;
			FrameBlock fb = fo.acquireRead();
			String[][] frame = DataConverter.convertToStringFrame(fb);
			fo.release();
			return frame;
		} catch (CacheException e) {
			throw new MLContextException("Cache exception when reading frame", e);
		} catch (DMLRuntimeException e) {
			throw new MLContextException("DML runtime exception when reading frame", e);
		}
	}

	/**
	 * Obtain a {@code double} output
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code double}
	 */
	public double getDouble(String outputName) {
		ScalarObject so = getScalarObject(outputName);
		return so.getDoubleValue();
	}

	/**
	 * Obtain an output as a {@code Scalar} object.
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code Scalar} object
	 */
	public ScalarObject getScalarObject(String outputName) {
		Data data = getData(outputName);
		if (!(data instanceof ScalarObject)) {
			throw new MLContextException("Variable '" + outputName + "' not a scalar");
		}
		ScalarObject so = (ScalarObject) data;
		return so;
	}

	/**
	 * Obtain a {@code boolean} output
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code boolean}
	 */
	public boolean getBoolean(String outputName) {
		ScalarObject so = getScalarObject(outputName);
		return so.getBooleanValue();
	}

	/**
	 * Obtain a {@code long} output
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code long}
	 */
	public long getLong(String outputName) {
		ScalarObject so = getScalarObject(outputName);
		return so.getLongValue();
	}

	/**
	 * Obtain a {@code String} output
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output as a {@code String}
	 */
	public String getString(String outputName) {
		ScalarObject so = getScalarObject(outputName);
		return so.getStringValue();
	}

	/**
	 * Obtain the Script object associated with these results.
	 * 
	 * @return the DML or PYDML Script object
	 */
	public Script getScript() {
		return script;
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T> Tuple1<T> getTuple(String outputName1) {
		return new Tuple1<T>((T) outputValue(outputName1));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2> Tuple2<T1, T2> getTuple(String outputName1, String outputName2) {
		return new Tuple2<T1, T2>((T1) outputValue(outputName1), (T2) outputValue(outputName2));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3> Tuple3<T1, T2, T3> getTuple(String outputName1, String outputName2, String outputName3) {
		return new Tuple3<T1, T2, T3>((T1) outputValue(outputName1), (T2) outputValue(outputName2),
				(T3) outputValue(outputName3));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4> Tuple4<T1, T2, T3, T4> getTuple(String outputName1, String outputName2, String outputName3,
			String outputName4) {
		return new Tuple4<T1, T2, T3, T4>((T1) outputValue(outputName1), (T2) outputValue(outputName2),
				(T3) outputValue(outputName3), (T4) outputValue(outputName4));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5> Tuple5<T1, T2, T3, T4, T5> getTuple(String outputName1, String outputName2,
			String outputName3, String outputName4, String outputName5) {
		return new Tuple5<T1, T2, T3, T4, T5>((T1) outputValue(outputName1), (T2) outputValue(outputName2),
				(T3) outputValue(outputName3), (T4) outputValue(outputName4), (T5) outputValue(outputName5));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6> Tuple6<T1, T2, T3, T4, T5, T6> getTuple(String outputName1, String outputName2,
			String outputName3, String outputName4, String outputName5, String outputName6) {
		return new Tuple6<T1, T2, T3, T4, T5, T6>((T1) outputValue(outputName1), (T2) outputValue(outputName2),
				(T3) outputValue(outputName3), (T4) outputValue(outputName4), (T5) outputValue(outputName5),
				(T6) outputValue(outputName6));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7> Tuple7<T1, T2, T3, T4, T5, T6, T7> getTuple(String outputName1,
			String outputName2, String outputName3, String outputName4, String outputName5, String outputName6,
			String outputName7) {
		return new Tuple7<T1, T2, T3, T4, T5, T6, T7>((T1) outputValue(outputName1), (T2) outputValue(outputName2),
				(T3) outputValue(outputName3), (T4) outputValue(outputName4), (T5) outputValue(outputName5),
				(T6) outputValue(outputName6), (T7) outputValue(outputName7));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8> Tuple8<T1, T2, T3, T4, T5, T6, T7, T8> getTuple(String outputName1,
			String outputName2, String outputName3, String outputName4, String outputName5, String outputName6,
			String outputName7, String outputName8) {
		return new Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>((T1) outputValue(outputName1), (T2) outputValue(outputName2),
				(T3) outputValue(outputName3), (T4) outputValue(outputName4), (T5) outputValue(outputName5),
				(T6) outputValue(outputName6), (T7) outputValue(outputName7), (T8) outputValue(outputName8));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9> Tuple9<T1, T2, T3, T4, T5, T6, T7, T8, T9> getTuple(String outputName1,
			String outputName2, String outputName3, String outputName4, String outputName5, String outputName6,
			String outputName7, String outputName8, String outputName9) {
		return new Tuple9<T1, T2, T3, T4, T5, T6, T7, T8, T9>((T1) outputValue(outputName1),
				(T2) outputValue(outputName2), (T3) outputValue(outputName3), (T4) outputValue(outputName4),
				(T5) outputValue(outputName5), (T6) outputValue(outputName6), (T7) outputValue(outputName7),
				(T8) outputValue(outputName8), (T9) outputValue(outputName9));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> Tuple10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10) {
		return new Tuple10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>((T1) outputValue(outputName1),
				(T2) outputValue(outputName2), (T3) outputValue(outputName3), (T4) outputValue(outputName4),
				(T5) outputValue(outputName5), (T6) outputValue(outputName6), (T7) outputValue(outputName7),
				(T8) outputValue(outputName8), (T9) outputValue(outputName9), (T10) outputValue(outputName10));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> Tuple11<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11) {
		return new Tuple11<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>((T1) outputValue(outputName1),
				(T2) outputValue(outputName2), (T3) outputValue(outputName3), (T4) outputValue(outputName4),
				(T5) outputValue(outputName5), (T6) outputValue(outputName6), (T7) outputValue(outputName7),
				(T8) outputValue(outputName8), (T9) outputValue(outputName9), (T10) outputValue(outputName10),
				(T11) outputValue(outputName11));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> Tuple12<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12) {
		return new Tuple12<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>((T1) outputValue(outputName1),
				(T2) outputValue(outputName2), (T3) outputValue(outputName3), (T4) outputValue(outputName4),
				(T5) outputValue(outputName5), (T6) outputValue(outputName6), (T7) outputValue(outputName7),
				(T8) outputValue(outputName8), (T9) outputValue(outputName9), (T10) outputValue(outputName10),
				(T11) outputValue(outputName11), (T12) outputValue(outputName12));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @param outputName13
	 *            the name of the thirteenth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> Tuple13<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12, String outputName13) {
		return new Tuple13<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>((T1) outputValue(outputName1),
				(T2) outputValue(outputName2), (T3) outputValue(outputName3), (T4) outputValue(outputName4),
				(T5) outputValue(outputName5), (T6) outputValue(outputName6), (T7) outputValue(outputName7),
				(T8) outputValue(outputName8), (T9) outputValue(outputName9), (T10) outputValue(outputName10),
				(T11) outputValue(outputName11), (T12) outputValue(outputName12), (T13) outputValue(outputName13));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @param outputName13
	 *            the name of the thirteenth output
	 * @param outputName14
	 *            the name of the fourteenth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> Tuple14<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12, String outputName13, String outputName14) {
		return new Tuple14<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>((T1) outputValue(outputName1),
				(T2) outputValue(outputName2), (T3) outputValue(outputName3), (T4) outputValue(outputName4),
				(T5) outputValue(outputName5), (T6) outputValue(outputName6), (T7) outputValue(outputName7),
				(T8) outputValue(outputName8), (T9) outputValue(outputName9), (T10) outputValue(outputName10),
				(T11) outputValue(outputName11), (T12) outputValue(outputName12), (T13) outputValue(outputName13),
				(T14) outputValue(outputName14));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @param outputName13
	 *            the name of the thirteenth output
	 * @param outputName14
	 *            the name of the fourteenth output
	 * @param outputName15
	 *            the name of the fifteenth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> Tuple15<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12, String outputName13, String outputName14, String outputName15) {
		return new Tuple15<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>(
				(T1) outputValue(outputName1), (T2) outputValue(outputName2), (T3) outputValue(outputName3),
				(T4) outputValue(outputName4), (T5) outputValue(outputName5), (T6) outputValue(outputName6),
				(T7) outputValue(outputName7), (T8) outputValue(outputName8), (T9) outputValue(outputName9),
				(T10) outputValue(outputName10), (T11) outputValue(outputName11), (T12) outputValue(outputName12),
				(T13) outputValue(outputName13), (T14) outputValue(outputName14), (T15) outputValue(outputName15));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @param outputName13
	 *            the name of the thirteenth output
	 * @param outputName14
	 *            the name of the fourteenth output
	 * @param outputName15
	 *            the name of the fifteenth output
	 * @param outputName16
	 *            the name of the sixteenth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> Tuple16<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12, String outputName13, String outputName14, String outputName15,
			String outputName16) {
		return new Tuple16<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>(
				(T1) outputValue(outputName1), (T2) outputValue(outputName2), (T3) outputValue(outputName3),
				(T4) outputValue(outputName4), (T5) outputValue(outputName5), (T6) outputValue(outputName6),
				(T7) outputValue(outputName7), (T8) outputValue(outputName8), (T9) outputValue(outputName9),
				(T10) outputValue(outputName10), (T11) outputValue(outputName11), (T12) outputValue(outputName12),
				(T13) outputValue(outputName13), (T14) outputValue(outputName14), (T15) outputValue(outputName15),
				(T16) outputValue(outputName16));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @param outputName13
	 *            the name of the thirteenth output
	 * @param outputName14
	 *            the name of the fourteenth output
	 * @param outputName15
	 *            the name of the fifteenth output
	 * @param outputName16
	 *            the name of the sixteenth output
	 * @param outputName17
	 *            the name of the seventeenth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> Tuple17<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12, String outputName13, String outputName14, String outputName15,
			String outputName16, String outputName17) {
		return new Tuple17<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>(
				(T1) outputValue(outputName1), (T2) outputValue(outputName2), (T3) outputValue(outputName3),
				(T4) outputValue(outputName4), (T5) outputValue(outputName5), (T6) outputValue(outputName6),
				(T7) outputValue(outputName7), (T8) outputValue(outputName8), (T9) outputValue(outputName9),
				(T10) outputValue(outputName10), (T11) outputValue(outputName11), (T12) outputValue(outputName12),
				(T13) outputValue(outputName13), (T14) outputValue(outputName14), (T15) outputValue(outputName15),
				(T16) outputValue(outputName16), (T17) outputValue(outputName17));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @param outputName13
	 *            the name of the thirteenth output
	 * @param outputName14
	 *            the name of the fourteenth output
	 * @param outputName15
	 *            the name of the fifteenth output
	 * @param outputName16
	 *            the name of the sixteenth output
	 * @param outputName17
	 *            the name of the seventeenth output
	 * @param outputName18
	 *            the name of the eighteenth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> Tuple18<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12, String outputName13, String outputName14, String outputName15,
			String outputName16, String outputName17, String outputName18) {
		return new Tuple18<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>(
				(T1) outputValue(outputName1), (T2) outputValue(outputName2), (T3) outputValue(outputName3),
				(T4) outputValue(outputName4), (T5) outputValue(outputName5), (T6) outputValue(outputName6),
				(T7) outputValue(outputName7), (T8) outputValue(outputName8), (T9) outputValue(outputName9),
				(T10) outputValue(outputName10), (T11) outputValue(outputName11), (T12) outputValue(outputName12),
				(T13) outputValue(outputName13), (T14) outputValue(outputName14), (T15) outputValue(outputName15),
				(T16) outputValue(outputName16), (T17) outputValue(outputName17), (T18) outputValue(outputName18));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @param outputName13
	 *            the name of the thirteenth output
	 * @param outputName14
	 *            the name of the fourteenth output
	 * @param outputName15
	 *            the name of the fifteenth output
	 * @param outputName16
	 *            the name of the sixteenth output
	 * @param outputName17
	 *            the name of the seventeenth output
	 * @param outputName18
	 *            the name of the eighteenth output
	 * @param outputName19
	 *            the name of the nineteenth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> Tuple19<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12, String outputName13, String outputName14, String outputName15,
			String outputName16, String outputName17, String outputName18, String outputName19) {
		return new Tuple19<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>(
				(T1) outputValue(outputName1), (T2) outputValue(outputName2), (T3) outputValue(outputName3),
				(T4) outputValue(outputName4), (T5) outputValue(outputName5), (T6) outputValue(outputName6),
				(T7) outputValue(outputName7), (T8) outputValue(outputName8), (T9) outputValue(outputName9),
				(T10) outputValue(outputName10), (T11) outputValue(outputName11), (T12) outputValue(outputName12),
				(T13) outputValue(outputName13), (T14) outputValue(outputName14), (T15) outputValue(outputName15),
				(T16) outputValue(outputName16), (T17) outputValue(outputName17), (T18) outputValue(outputName18),
				(T19) outputValue(outputName19));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @param outputName13
	 *            the name of the thirteenth output
	 * @param outputName14
	 *            the name of the fourteenth output
	 * @param outputName15
	 *            the name of the fifteenth output
	 * @param outputName16
	 *            the name of the sixteenth output
	 * @param outputName17
	 *            the name of the seventeenth output
	 * @param outputName18
	 *            the name of the eighteenth output
	 * @param outputName19
	 *            the name of the nineteenth output
	 * @param outputName20
	 *            the name of the twentieth output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> Tuple20<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12, String outputName13, String outputName14, String outputName15,
			String outputName16, String outputName17, String outputName18, String outputName19, String outputName20) {
		return new Tuple20<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>(
				(T1) outputValue(outputName1), (T2) outputValue(outputName2), (T3) outputValue(outputName3),
				(T4) outputValue(outputName4), (T5) outputValue(outputName5), (T6) outputValue(outputName6),
				(T7) outputValue(outputName7), (T8) outputValue(outputName8), (T9) outputValue(outputName9),
				(T10) outputValue(outputName10), (T11) outputValue(outputName11), (T12) outputValue(outputName12),
				(T13) outputValue(outputName13), (T14) outputValue(outputName14), (T15) outputValue(outputName15),
				(T16) outputValue(outputName16), (T17) outputValue(outputName17), (T18) outputValue(outputName18),
				(T19) outputValue(outputName19), (T20) outputValue(outputName20));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @param outputName13
	 *            the name of the thirteenth output
	 * @param outputName14
	 *            the name of the fourteenth output
	 * @param outputName15
	 *            the name of the fifteenth output
	 * @param outputName16
	 *            the name of the sixteenth output
	 * @param outputName17
	 *            the name of the seventeenth output
	 * @param outputName18
	 *            the name of the eighteenth output
	 * @param outputName19
	 *            the name of the nineteenth output
	 * @param outputName20
	 *            the name of the twentieth output
	 * @param outputName21
	 *            the name of the twenty-first output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> Tuple21<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12, String outputName13, String outputName14, String outputName15,
			String outputName16, String outputName17, String outputName18, String outputName19, String outputName20,
			String outputName21) {
		return new Tuple21<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>(
				(T1) outputValue(outputName1), (T2) outputValue(outputName2), (T3) outputValue(outputName3),
				(T4) outputValue(outputName4), (T5) outputValue(outputName5), (T6) outputValue(outputName6),
				(T7) outputValue(outputName7), (T8) outputValue(outputName8), (T9) outputValue(outputName9),
				(T10) outputValue(outputName10), (T11) outputValue(outputName11), (T12) outputValue(outputName12),
				(T13) outputValue(outputName13), (T14) outputValue(outputName14), (T15) outputValue(outputName15),
				(T16) outputValue(outputName16), (T17) outputValue(outputName17), (T18) outputValue(outputName18),
				(T19) outputValue(outputName19), (T20) outputValue(outputName20), (T21) outputValue(outputName21));
	}

	/**
	 * Obtain a Scala tuple.
	 * 
	 * @param outputName1
	 *            the name of the first output
	 * @param outputName2
	 *            the name of the second output
	 * @param outputName3
	 *            the name of the third output
	 * @param outputName4
	 *            the name of the fourth output
	 * @param outputName5
	 *            the name of the fifth output
	 * @param outputName6
	 *            the name of the sixth output
	 * @param outputName7
	 *            the name of the seventh output
	 * @param outputName8
	 *            the name of the eighth output
	 * @param outputName9
	 *            the name of the ninth output
	 * @param outputName10
	 *            the name of the tenth output
	 * @param outputName11
	 *            the name of the eleventh output
	 * @param outputName12
	 *            the name of the twelfth output
	 * @param outputName13
	 *            the name of the thirteenth output
	 * @param outputName14
	 *            the name of the fourteenth output
	 * @param outputName15
	 *            the name of the fifteenth output
	 * @param outputName16
	 *            the name of the sixteenth output
	 * @param outputName17
	 *            the name of the seventeenth output
	 * @param outputName18
	 *            the name of the eighteenth output
	 * @param outputName19
	 *            the name of the nineteenth output
	 * @param outputName20
	 *            the name of the twentieth output
	 * @param outputName21
	 *            the name of the twenty-first output
	 * @param outputName22
	 *            the name of the twenty-second output
	 * @return a Scala tuple
	 */
	@SuppressWarnings("unchecked")
	public <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22> Tuple22<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22> getTuple(
			String outputName1, String outputName2, String outputName3, String outputName4, String outputName5,
			String outputName6, String outputName7, String outputName8, String outputName9, String outputName10,
			String outputName11, String outputName12, String outputName13, String outputName14, String outputName15,
			String outputName16, String outputName17, String outputName18, String outputName19, String outputName20,
			String outputName21, String outputName22) {
		return new Tuple22<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>(
				(T1) outputValue(outputName1), (T2) outputValue(outputName2), (T3) outputValue(outputName3),
				(T4) outputValue(outputName4), (T5) outputValue(outputName5), (T6) outputValue(outputName6),
				(T7) outputValue(outputName7), (T8) outputValue(outputName8), (T9) outputValue(outputName9),
				(T10) outputValue(outputName10), (T11) outputValue(outputName11), (T12) outputValue(outputName12),
				(T13) outputValue(outputName13), (T14) outputValue(outputName14), (T15) outputValue(outputName15),
				(T16) outputValue(outputName16), (T17) outputValue(outputName17), (T18) outputValue(outputName18),
				(T19) outputValue(outputName19), (T20) outputValue(outputName20), (T21) outputValue(outputName21),
				(T22) outputValue(outputName22));
	}

	/**
	 * Provide support for Scala tuples by returning an output value cast to a
	 * specific output type. MLResults tuple support requires specifying the
	 * object types at runtime to avoid the items in the tuple being returned as
	 * Anys.
	 * 
	 * @param outputName
	 *            the name of the output
	 * @return the output value cast to a specific output type
	 */
	@SuppressWarnings("unchecked")
	private <T> T outputValue(String outputName) {
		Data data = getData(outputName);
		if (data instanceof BooleanObject) {
			return (T) new Boolean(((BooleanObject) data).getBooleanValue());
		} else if (data instanceof DoubleObject) {
			return (T) new Double(((DoubleObject) data).getDoubleValue());
		} else if (data instanceof IntObject) {
			return (T) new Long(((IntObject) data).getLongValue());
		} else if (data instanceof StringObject) {
			return (T) ((StringObject) data).getStringValue();
		} else if (data instanceof MatrixObject) {
			return (T) getMatrix(outputName);
		} else if (data instanceof FrameObject) {
			return (T) getFrame(outputName);
		}
		return (T) data;
	}

	/**
	 * Obtain the symbol table, which is essentially a {@code Map<String, Data>}
	 * representing variables and their values as SystemML representations.
	 * 
	 * @return the symbol table
	 */
	public LocalVariableMap getSymbolTable() {
		return symbolTable;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(MLContextUtil.displayOutputs(script.getOutputVariables(), symbolTable));
		return sb.toString();
	}

}
