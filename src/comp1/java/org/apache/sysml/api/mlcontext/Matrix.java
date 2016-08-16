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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;

/**
 * Matrix encapsulates a SystemML matrix. It allows for easy conversion to
 * various other formats, such as RDDs, JavaRDDs, DataFrames,
 * BinaryBlockMatrices, and double[][]s. After script execution, it offers a
 * convenient format for obtaining SystemML matrix data in Scala tuples.
 *
 */
public class Matrix {

	private MatrixObject matrixObject;
	private SparkExecutionContext sparkExecutionContext;

	public Matrix(MatrixObject matrixObject, SparkExecutionContext sparkExecutionContext) {
		this.matrixObject = matrixObject;
		this.sparkExecutionContext = sparkExecutionContext;
	}

	/**
	 * Obtain the matrix as a SystemML MatrixObject.
	 * 
	 * @return the matrix as a SystemML MatrixObject
	 */
	public MatrixObject asMatrixObject() {
		return matrixObject;
	}

	/**
	 * Obtain the matrix as a two-dimensional double array
	 * 
	 * @return the matrix as a two-dimensional double array
	 */
	public double[][] asDoubleMatrix() {
		double[][] doubleMatrix = MLContextConversionUtil.matrixObjectToDoubleMatrix(matrixObject);
		return doubleMatrix;
	}

	/**
	 * Obtain the matrix as a {@code JavaRDD<String>} in IJV format
	 * 
	 * @return the matrix as a {@code JavaRDD<String>} in IJV format
	 */
	public JavaRDD<String> asJavaRDDStringIJV() {
		JavaRDD<String> javaRDDStringIJV = MLContextConversionUtil.matrixObjectToJavaRDDStringIJV(matrixObject);
		return javaRDDStringIJV;
	}

	/**
	 * Obtain the matrix as a {@code JavaRDD<String>} in CSV format
	 * 
	 * @return the matrix as a {@code JavaRDD<String>} in CSV format
	 */
	public JavaRDD<String> asJavaRDDStringCSV() {
		JavaRDD<String> javaRDDStringCSV = MLContextConversionUtil.matrixObjectToJavaRDDStringCSV(matrixObject);
		return javaRDDStringCSV;
	}

	/**
	 * Obtain the matrix as a {@code RDD<String>} in CSV format
	 * 
	 * @return the matrix as a {@code RDD<String>} in CSV format
	 */
	public RDD<String> asRDDStringCSV() {
		RDD<String> rddStringCSV = MLContextConversionUtil.matrixObjectToRDDStringCSV(matrixObject);
		return rddStringCSV;
	}

	/**
	 * Obtain the matrix as a {@code RDD<String>} in IJV format
	 * 
	 * @return the matrix as a {@code RDD<String>} in IJV format
	 */
	public RDD<String> asRDDStringIJV() {
		RDD<String> rddStringIJV = MLContextConversionUtil.matrixObjectToRDDStringIJV(matrixObject);
		return rddStringIJV;
	}

	/**
	 * Obtain the matrix as a {@code DataFrame}
	 * 
	 * @return the matrix as a {@code DataFrame}
	 */
	public DataFrame asDataFrame() {
		DataFrame df = MLContextConversionUtil.matrixObjectToDataFrame(matrixObject, sparkExecutionContext, false);
		return df;
	}

	/**
	 * Obtain the matrix as a {@code BinaryBlockMatrix}
	 * 
	 * @return the matrix as a {@code BinaryBlockMatrix}
	 */
	public BinaryBlockMatrix asBinaryBlockMatrix() {
		BinaryBlockMatrix binaryBlockMatrix = MLContextConversionUtil.matrixObjectToBinaryBlockMatrix(matrixObject,
				sparkExecutionContext);
		return binaryBlockMatrix;
	}

	/**
	 * Obtain the matrix metadata
	 * 
	 * @return the matrix metadata
	 */
	public MatrixMetadata getMatrixMetadata() {
		MatrixCharacteristics matrixCharacteristics = matrixObject.getMatrixCharacteristics();
		MatrixMetadata matrixMetadata = new MatrixMetadata(matrixCharacteristics);
		return matrixMetadata;
	}

	@Override
	public String toString() {
		return matrixObject.toString();
	}
}
