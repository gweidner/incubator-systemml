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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.io.Text;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import scala.Tuple2;

import org.apache.sysml.api.MLOutput.ConvertDoubleArrayToRows;
import org.apache.sysml.api.MLOutput.ProjectRows;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.spark.functions.ConvertMatrixBlockToIJVLines;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsExt.AddRowID;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsExt.DataFrameAnalysisFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsExt.IJVToBinaryBlockFunctionHelper;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsExt.RDDConverterTypes;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsExt.RowToBinaryBlockFunctionHelper;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixCell;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.mapred.ReblockBuffer;
import org.apache.sysml.runtime.util.FastStringTokenizer;
import org.apache.sysml.runtime.util.UtilFunctions;

/**
 * NOTE: These are experimental converter utils. Once thoroughly tested, they
 * can be moved to RDDConverterUtils.
 */
@SuppressWarnings("unused")
public class RDDConverterUtilsExtComp 
{
	
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> vectorDataFrameToBinaryBlock(SparkContext sc,
			Dataset<Row> inputDF, MatrixCharacteristics mcOut, boolean containsID, String vectorColumnName) throws DMLRuntimeException {
		return vectorDataFrameToBinaryBlock(new JavaSparkContext(sc), inputDF, mcOut, containsID, vectorColumnName);
	}
	
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> vectorDataFrameToBinaryBlock(JavaSparkContext sc,
			Dataset<Row> inputDF, MatrixCharacteristics mcOut, boolean containsID, String vectorColumnName)
			throws DMLRuntimeException {
		
		if(containsID) {
			inputDF = dropColumn(inputDF.sort("ID"), "ID");
		}
		
		Dataset<Row> df = inputDF.select(vectorColumnName);
			
		//determine unknown dimensions and sparsity if required
		if( !mcOut.dimsKnown(true) ) {
			Accumulator<Double> aNnz = sc.accumulator(0L);
			JavaRDD<Row> tmp = df.javaRDD().map(new DataFrameAnalysisFunction(aNnz, true));
			long rlen = tmp.count();
			long clen = ((Vector) tmp.first().get(0)).size();
			long nnz = UtilFunctions.toLong(aNnz.value());
			mcOut.set(rlen, clen, mcOut.getRowsPerBlock(), mcOut.getColsPerBlock(), nnz);
		}
		
		JavaPairRDD<Row, Long> prepinput = df.javaRDD()
				.zipWithIndex(); //zip row index
		
		//convert csv rdd to binary block rdd (w/ partial blocks)
		JavaPairRDD<MatrixIndexes, MatrixBlock> out = 
				prepinput.mapPartitionsToPair(
					new DataFrameToBinaryBlockFunction(mcOut, true));
		
		//aggregate partial matrix blocks
		out = RDDAggregateUtils.mergeByKey( out ); 
		
		return out;
	}
	
	/**
	 * Adding utility to support for dropping columns for older Spark versions.
	 * @param df
	 * @param column
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static Dataset<Row> dropColumn(Dataset<Row> df, String column) throws DMLRuntimeException {
		ArrayList<String> columnToSelect = new ArrayList<String>();
		String firstCol = null;
		boolean colPresent = false;
		for(String col : df.columns()) {
			if(col.equals(column)) {
				colPresent = true;
			}
			else if(firstCol == null) {
				firstCol = col;
			}
			else {
				columnToSelect.add(col);
			}
		}
		
		if(!colPresent) {
			throw new DMLRuntimeException("The column \"" + column + "\" is not present in the dataframe.");
		}
		else if(firstCol == null) {
			throw new DMLRuntimeException("No column other than \"" + column + "\" present in the dataframe.");
		}
		
		// Round about way to do in Java (not exposed in Spark 1.3.0): df = df.drop("ID");
		return df.select(firstCol, scala.collection.JavaConversions.asScalaBuffer(columnToSelect).toList());
	}
	
	public static Dataset<Row> projectColumns(Dataset<Row> df, ArrayList<String> columns) throws DMLRuntimeException {
		ArrayList<String> columnToSelect = new ArrayList<String>();
		for(int i = 1; i < columns.size(); i++) {
			columnToSelect.add(columns.get(i));
		}
		return df.select(columns.get(0), scala.collection.JavaConversions.asScalaBuffer(columnToSelect).toList());
	}
	
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> dataFrameToBinaryBlock(SparkContext sc,
			Dataset<Row> df, MatrixCharacteristics mcOut, boolean containsID) throws DMLRuntimeException {
		return dataFrameToBinaryBlock(new JavaSparkContext(sc), df, mcOut, containsID, null);
	}
	
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> dataFrameToBinaryBlock(SparkContext sc,
			Dataset<Row> df, MatrixCharacteristics mcOut, String [] columns) throws DMLRuntimeException {
		ArrayList<String> columns1 = new ArrayList<String>(Arrays.asList(columns));
		return dataFrameToBinaryBlock(new JavaSparkContext(sc), df, mcOut, false, columns1);
	}
	
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> dataFrameToBinaryBlock(SparkContext sc,
			Dataset<Row> df, MatrixCharacteristics mcOut, ArrayList<String> columns) throws DMLRuntimeException {
		return dataFrameToBinaryBlock(new JavaSparkContext(sc), df, mcOut, false, columns);
	}
	
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> dataFrameToBinaryBlock(SparkContext sc,
			Dataset<Row> df, MatrixCharacteristics mcOut, boolean containsID, String [] columns) 
			throws DMLRuntimeException {
		ArrayList<String> columns1 = new ArrayList<String>(Arrays.asList(columns));
		return dataFrameToBinaryBlock(new JavaSparkContext(sc), df, mcOut, containsID, columns1);
	}
	
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> dataFrameToBinaryBlock(SparkContext sc,
			Dataset<Row> df, MatrixCharacteristics mcOut, boolean containsID, ArrayList<String> columns) 
			throws DMLRuntimeException {
		return dataFrameToBinaryBlock(new JavaSparkContext(sc), df, mcOut, containsID, columns);
	}
	
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> dataFrameToBinaryBlock(JavaSparkContext sc,
			Dataset<Row> df, MatrixCharacteristics mcOut, boolean containsID) throws DMLRuntimeException {
		return dataFrameToBinaryBlock(sc, df, mcOut, containsID, null);
	}
	
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> dataFrameToBinaryBlock(JavaSparkContext sc,
			Dataset<Row> df, MatrixCharacteristics mcOut, ArrayList<String> columns) throws DMLRuntimeException {
		return dataFrameToBinaryBlock(sc, df, mcOut, false, columns);
	}
	
	/**
	 * Converts DataFrame into binary blocked RDD. 
	 * Note: mcOut will be set if you don't know the dimensions.
	 * @param sc
	 * @param df
	 * @param mcOut
	 * @param containsID
	 * @param columns
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static JavaPairRDD<MatrixIndexes, MatrixBlock> dataFrameToBinaryBlock(JavaSparkContext sc,
			Dataset<Row> df, MatrixCharacteristics mcOut, boolean containsID, ArrayList<String> columns) 
			throws DMLRuntimeException {
		if(columns != null) {
			df = projectColumns(df, columns);
		}
		
		if(containsID) {
			df = dropColumn(df.sort("ID"), "ID");
		}
			
		//determine unknown dimensions and sparsity if required
		if( !mcOut.dimsKnown(true) ) {
			Accumulator<Double> aNnz = sc.accumulator(0L);
			JavaRDD<Row> tmp = df.javaRDD().map(new DataFrameAnalysisFunction(aNnz, false));
			long rlen = tmp.count();
			long clen = containsID ? (df.columns().length - 1) : df.columns().length;
			long nnz = UtilFunctions.toLong(aNnz.value());
			mcOut.set(rlen, clen, mcOut.getRowsPerBlock(), mcOut.getColsPerBlock(), nnz);
		}
		
		JavaPairRDD<Row, Long> prepinput = df.javaRDD()
				.zipWithIndex(); //zip row index
		
		//convert csv rdd to binary block rdd (w/ partial blocks)
		JavaPairRDD<MatrixIndexes, MatrixBlock> out = 
				prepinput.mapPartitionsToPair(
					new DataFrameToBinaryBlockFunction(mcOut, false));
		
		//aggregate partial matrix blocks
		out = RDDAggregateUtils.mergeByKey( out ); 
		
		return out;
	}
	
	public static Dataset<Row> binaryBlockToVectorDataFrame(JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlockRDD, 
			MatrixCharacteristics mc, SQLContext sqlContext) throws DMLRuntimeException {
		long rlen = mc.getRows(); long clen = mc.getCols();
		int brlen = mc.getRowsPerBlock(); int bclen = mc.getColsPerBlock();
		// Very expensive operation here: groupByKey (where number of keys might be too large)
		JavaRDD<Row> rowsRDD = binaryBlockRDD.flatMapToPair(new ProjectRows(rlen, clen, brlen, bclen))
				.groupByKey().map(new ConvertDoubleArrayToRows(clen, bclen, true));
		
		int numColumns = (int) clen;
		if(numColumns <= 0) {
			throw new DMLRuntimeException("Output dimensions unknown after executing the script and hence cannot create the dataframe");
		}
		
		List<StructField> fields = new ArrayList<StructField>();
		// LongTypes throw an error: java.lang.Double incompatible with java.lang.Long
		fields.add(DataTypes.createStructField("ID", DataTypes.DoubleType, false));
		fields.add(DataTypes.createStructField("C1", new VectorUDT(), false));
		// fields.add(DataTypes.createStructField("C1", DataTypes.createArrayType(DataTypes.DoubleType), false));
		
		// This will cause infinite recursion due to bug in Spark
		// https://issues.apache.org/jira/browse/SPARK-6999
		// return sqlContext.createDataFrame(rowsRDD, colNames); // where ArrayList<String> colNames
		return sqlContext.createDataFrame(rowsRDD.rdd(), DataTypes.createStructType(fields));
	}
	
	/**
	 * Add element indices as new column to DataFrame
	 * Note: Element indices start from 1
	 * @param df input data frame
	 * @param nameOfCol name of index column
	 * @return new data frame
	 */
	public static Dataset<Row> addIDToDataFrame(Dataset<Row> df, SQLContext sqlContext, String nameOfCol) {
		StructField[] oldSchema = df.schema().fields();
		StructField[] newSchema = new StructField[oldSchema.length + 1];
		for(int i = 0; i < oldSchema.length; i++) {
			newSchema[i] = oldSchema[i];
		}
		newSchema[oldSchema.length] = DataTypes.createStructField(nameOfCol, DataTypes.DoubleType, false);
		// JavaRDD<Row> newRows = df.rdd().toJavaRDD().map(new AddRowID());
		JavaRDD<Row> newRows = df.rdd().toJavaRDD().zipWithIndex().map(new AddRowID());
		return sqlContext.createDataFrame(newRows, new StructType(newSchema));
	}
	
	public static Dataset<Row> binaryBlockToDataFrame(JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlockRDD, 
			MatrixCharacteristics mc, SQLContext sqlContext) throws DMLRuntimeException {
		long rlen = mc.getRows(); long clen = mc.getCols();
		int brlen = mc.getRowsPerBlock(); int bclen = mc.getColsPerBlock();
		
		// Very expensive operation here: groupByKey (where number of keys might be too large)
		JavaRDD<Row> rowsRDD = binaryBlockRDD.flatMapToPair(new ProjectRows(rlen, clen, brlen, bclen))
				.groupByKey().map(new ConvertDoubleArrayToRows(clen, bclen, false));
		
		int numColumns = (int) clen;
		if(numColumns <= 0) {
			// numColumns = rowsRDD.first().length() - 1; // Ugly, so instead prefer to throw
			throw new DMLRuntimeException("Output dimensions unknown after executing the script and hence cannot create the dataframe");
		}
		
		List<StructField> fields = new ArrayList<StructField>();
		// LongTypes throw an error: java.lang.Double incompatible with java.lang.Long
		fields.add(DataTypes.createStructField("ID", DataTypes.DoubleType, false)); 
		for(int i = 1; i <= numColumns; i++) {
			fields.add(DataTypes.createStructField("C" + i, DataTypes.DoubleType, false));
		}
		
		// This will cause infinite recursion due to bug in Spark
		// https://issues.apache.org/jira/browse/SPARK-6999
		// return sqlContext.createDataFrame(rowsRDD, colNames); // where ArrayList<String> colNames
		return sqlContext.createDataFrame(rowsRDD.rdd(), DataTypes.createStructType(fields));
	}
	
	/*private*/ static class MatrixEntryToBinaryBlockFunction implements PairFlatMapFunction<Iterator<MatrixEntry>,MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = 4907483236186747224L;
		
		private IJVToBinaryBlockFunctionHelper helper = null;
		public MatrixEntryToBinaryBlockFunction(MatrixCharacteristics mc) throws DMLRuntimeException {
			helper = new IJVToBinaryBlockFunctionHelper(mc);
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<MatrixEntry> arg0) throws Exception {
			return helper.convertToBinaryBlock(arg0, RDDConverterTypes.MATRIXENTRY_TO_MATRIXCELL).iterator();
		}

	}
	
	private static class CSVToBinaryBlockFunction implements PairFlatMapFunction<Iterator<Tuple2<Text,Long>>,MatrixIndexes,MatrixBlock> {
		private static final long serialVersionUID = 1501589201971233542L;
		
		private RowToBinaryBlockFunctionHelper helper = null; 
		
		public CSVToBinaryBlockFunction(MatrixCharacteristics mc, String delim, boolean fill, double fillValue) {
			helper = new RowToBinaryBlockFunctionHelper(mc, delim, fill, fillValue);
		}
		
		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<Text, Long>> arg0) throws Exception {
			return helper.convertToBinaryBlock(arg0, RDDConverterTypes.TEXT_TO_DOUBLEARR).iterator();
		}
		
	}
	
	public static class DataFrameToBinaryBlockFunction implements PairFlatMapFunction<Iterator<Tuple2<Row,Long>>,MatrixIndexes,MatrixBlock> {
		private static final long serialVersionUID = 653447740362447236L;
		private RowToBinaryBlockFunctionHelper helper = null; 
		boolean isVectorBasedDF;
		
		public DataFrameToBinaryBlockFunction(MatrixCharacteristics mc, boolean isVectorBasedDF) {
			helper = new RowToBinaryBlockFunctionHelper(mc);
			this.isVectorBasedDF = isVectorBasedDF;
		}
		
		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<Row, Long>> arg0) throws Exception {
			if(isVectorBasedDF)
				return helper.convertToBinaryBlock(arg0, RDDConverterTypes.VECTOR_TO_DOUBLEARR).iterator();
			else
				return helper.convertToBinaryBlock(arg0, RDDConverterTypes.ROW_TO_DOUBLEARR).iterator();
		}
		
	}
	
}
