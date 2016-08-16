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

import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.sysml.conf.CompilerConfig;
import org.apache.sysml.conf.CompilerConfig.ConfigType;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.parser.ParseException;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.controlprogram.caching.CacheException;
import org.apache.sysml.runtime.controlprogram.caching.FrameObject;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.cp.BooleanObject;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.sysml.runtime.instructions.cp.DoubleObject;
import org.apache.sysml.runtime.instructions.cp.IntObject;
import org.apache.sysml.runtime.instructions.cp.StringObject;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.MatrixFormatMetaData;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OutputInfo;

/**
 * Utility class containing methods for working with the MLContext API.
 *
 */
public final class MLContextUtil {

	/**
	 * Basic data types supported by the MLContext API
	 */
	@SuppressWarnings("rawtypes")
	public static final Class[] BASIC_DATA_TYPES = { Integer.class, Boolean.class, Double.class, String.class };

	/**
	 * Complex data types supported by the MLContext API
	 */
	@SuppressWarnings("rawtypes")
	public static final Class[] COMPLEX_DATA_TYPES = { JavaRDD.class, RDD.class, DataFrame.class,
			BinaryBlockMatrix.class, Matrix.class, (new double[][] {}).getClass(), MatrixBlock.class };

	/**
	 * All data types supported by the MLContext API
	 */
	@SuppressWarnings("rawtypes")
	public static final Class[] ALL_SUPPORTED_DATA_TYPES = (Class[]) ArrayUtils.addAll(BASIC_DATA_TYPES,
			COMPLEX_DATA_TYPES);

	/**
	 * Compare two version strings (ie, "1.4.0" and "1.4.1").
	 * 
	 * @param versionStr1
	 *            First version string.
	 * @param versionStr2
	 *            Second version string.
	 * @return If versionStr1 is less than versionStr2, return {@code -1}. If
	 *         versionStr1 equals versionStr2, return {@code 0}. If versionStr1
	 *         is greater than versionStr2, return {@code 1}.
	 * @throws MLContextException
	 *             if versionStr1 or versionStr2 is {@code null}
	 */
	private static int compareVersion(String versionStr1, String versionStr2) {
		if (versionStr1 == null) {
			throw new MLContextException("First version argument to compareVersion() is null");
		}
		if (versionStr2 == null) {
			throw new MLContextException("Second version argument to compareVersion() is null");
		}

		Scanner scanner1 = null;
		Scanner scanner2 = null;
		try {
			scanner1 = new Scanner(versionStr1);
			scanner2 = new Scanner(versionStr2);
			scanner1.useDelimiter("\\.");
			scanner2.useDelimiter("\\.");

			while (scanner1.hasNextInt() && scanner2.hasNextInt()) {
				int version1 = scanner1.nextInt();
				int version2 = scanner2.nextInt();
				if (version1 < version2) {
					return -1;
				} else if (version1 > version2) {
					return 1;
				}
			}

			return scanner1.hasNextInt() ? 1 : 0;
		} finally {
			scanner1.close();
			scanner2.close();
		}
	}

	/**
	 * Determine whether the Spark version is supported.
	 * 
	 * @param sparkVersion
	 *            Spark version string (ie, "1.5.0").
	 * @return {@code true} if Spark version supported; otherwise {@code false}.
	 */
	public static boolean isSparkVersionSupported(String sparkVersion) {
		if (compareVersion(sparkVersion, MLContext.SYSTEMML_MINIMUM_SPARK_VERSION) < 0) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Check that the Spark version is supported. If it isn't supported, throw
	 * an MLContextException.
	 * 
	 * @param sc
	 *            SparkContext
	 * @throws MLContextException
	 *             thrown if Spark version isn't supported
	 */
	public static void verifySparkVersionSupported(SparkContext sc) {
		if (!MLContextUtil.isSparkVersionSupported(sc.version())) {
			throw new MLContextException("SystemML requires Spark " + MLContext.SYSTEMML_MINIMUM_SPARK_VERSION
					+ " or greater");
		}
	}

	/**
	 * Set default SystemML configuration properties.
	 */
	public static void setDefaultConfig() {
		ConfigurationManager.setGlobalConfig(new DMLConfig());
	}

	/**
	 * Set SystemML configuration properties based on a configuration file.
	 * 
	 * @param configFilePath
	 *            Path to configuration file.
	 * @throws MLContextException
	 *             if configuration file was not found or a parse exception
	 *             occurred
	 */
	public static void setConfig(String configFilePath) {
		try {
			DMLConfig config = new DMLConfig(configFilePath);
			ConfigurationManager.setGlobalConfig(config);
		} catch (ParseException e) {
			throw new MLContextException("Parse Exception when setting config", e);
		} catch (FileNotFoundException e) {
			throw new MLContextException("File not found (" + configFilePath + ") when setting config", e);
		}
	}

	/**
	 * Set SystemML compiler configuration properties for MLContext
	 */
	public static void setCompilerConfig() {
		CompilerConfig compilerConfig = new CompilerConfig();
		compilerConfig.set(ConfigType.IGNORE_UNSPECIFIED_ARGS, true);
		compilerConfig.set(ConfigType.REJECT_READ_WRITE_UNKNOWNS, false);
		compilerConfig.set(ConfigType.ALLOW_CSE_PERSISTENT_READS, false);
		ConfigurationManager.setGlobalConfig(compilerConfig);
	}

	/**
	 * Convenience method to generate a {@code Map<String, Object>} of key/value
	 * pairs.
	 * <p>
	 * Example:<br>
	 * {@code Map<String, Object> inputMap = MLContextUtil.generateInputMap("A", 1, "B", "two", "C", 3);}
	 * <br>
	 * <br>
	 * This is equivalent to:<br>
	 * <code>Map&lt;String, Object&gt; inputMap = new LinkedHashMap&lt;String, Object&gt;(){{
	 *     <br>put("A", 1);
	 *     <br>put("B", "two");
	 *     <br>put("C", 3);
	 * <br>}};</code>
	 * 
	 * @param objs
	 *            List of String/Object pairs
	 * @return Map of String/Object pairs
	 * @throws MLContextException
	 *             if the number of arguments is not an even number
	 */
	public static Map<String, Object> generateInputMap(Object... objs) {
		int len = objs.length;
		if ((len & 1) == 1) {
			throw new MLContextException("The number of arguments needs to be an even number");
		}
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		int i = 0;
		while (i < len) {
			map.put((String) objs[i++], objs[i++]);
		}
		return map;
	}

	/**
	 * Verify that the types of input values are supported.
	 * 
	 * @param inputs
	 *            Map of String/Object pairs
	 * @throws MLContextException
	 *             if an input value type is not supported
	 */
	public static void checkInputValueTypes(Map<String, Object> inputs) {
		for (Entry<String, Object> entry : inputs.entrySet()) {
			checkInputValueType(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Verify that the type of input value is supported.
	 * 
	 * @param name
	 *            The name of the input
	 * @param value
	 *            The value of the input
	 * @throws MLContextException
	 *             if the input value type is not supported
	 */
	public static void checkInputValueType(String name, Object value) {

		if (name == null) {
			throw new MLContextException("No input name supplied");
		} else if (value == null) {
			throw new MLContextException("No input value supplied");
		}

		Object o = value;
		boolean supported = false;
		for (Class<?> clazz : ALL_SUPPORTED_DATA_TYPES) {
			if (o.getClass().equals(clazz)) {
				supported = true;
				break;
			} else if (clazz.isAssignableFrom(o.getClass())) {
				supported = true;
				break;
			}
		}
		if (!supported) {
			throw new MLContextException("Input name (\"" + value + "\") value type not supported: " + o.getClass());
		}
	}

	/**
	 * Verify that the type of input parameter value is supported.
	 * 
	 * @param parameterName
	 *            The name of the input parameter
	 * @param parameterValue
	 *            The value of the input parameter
	 * @throws MLContextException
	 *             if the input parameter value type is not supported
	 */
	public static void checkInputParameterType(String parameterName, Object parameterValue) {

		if (parameterName == null) {
			throw new MLContextException("No parameter name supplied");
		} else if (parameterValue == null) {
			throw new MLContextException("No parameter value supplied");
		} else if (!parameterName.startsWith("$")) {
			throw new MLContextException("Input parameter name must start with a $");
		}

		Object o = parameterValue;
		boolean supported = false;
		for (Class<?> clazz : BASIC_DATA_TYPES) {
			if (o.getClass().equals(clazz)) {
				supported = true;
				break;
			} else if (clazz.isAssignableFrom(o.getClass())) {
				supported = true;
				break;
			}
		}
		if (!supported) {
			throw new MLContextException("Input parameter (\"" + parameterName + "\") value type not supported: "
					+ o.getClass());
		}
	}

	/**
	 * Is the object one of the supported basic data types? (Integer, Boolean,
	 * Double, String)
	 * 
	 * @param object
	 *            the object type to be examined
	 * @return {@code true} if type is a basic data type; otherwise
	 *         {@code false}.
	 */
	public static boolean isBasicType(Object object) {
		for (Class<?> clazz : BASIC_DATA_TYPES) {
			if (object.getClass().equals(clazz)) {
				return true;
			} else if (clazz.isAssignableFrom(object.getClass())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Is the object one of the supported complex data types? (JavaRDD, RDD,
	 * DataFrame, BinaryBlockMatrix, Matrix, double[][])
	 * 
	 * @param object
	 *            the object type to be examined
	 * @return {@code true} if type is a complexe data type; otherwise
	 *         {@code false}.
	 */
	public static boolean isComplexType(Object object) {
		for (Class<?> clazz : COMPLEX_DATA_TYPES) {
			if (object.getClass().equals(clazz)) {
				return true;
			} else if (clazz.isAssignableFrom(object.getClass())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Converts non-string basic input parameter values to strings to pass to
	 * the parser.
	 * 
	 * @param basicInputParameterMap
	 *            map of input parameters
	 * @param scriptType
	 *            {@code ScriptType.DML} or {@code ScriptType.PYDML}
	 * @return map of String/String name/value pairs
	 */
	public static Map<String, String> convertInputParametersForParser(Map<String, Object> basicInputParameterMap,
			ScriptType scriptType) {
		if (basicInputParameterMap == null) {
			return null;
		}
		if (scriptType == null) {
			throw new MLContextException("ScriptType needs to be specified");
		}
		Map<String, String> convertedMap = new HashMap<String, String>();
		for (Entry<String, Object> entry : basicInputParameterMap.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (value == null) {
				throw new MLContextException("Input parameter value is null for: " + entry.getKey());
			} else if (value instanceof Integer) {
				convertedMap.put(key, Integer.toString((Integer) value));
			} else if (value instanceof Boolean) {
				if (scriptType == ScriptType.DML) {
					convertedMap.put(key, String.valueOf((Boolean) value).toUpperCase());
				} else {
					convertedMap.put(key, WordUtils.capitalize(String.valueOf((Boolean) value)));
				}
			} else if (value instanceof Double) {
				convertedMap.put(key, Double.toString((Double) value));
			} else if (value instanceof String) {
				convertedMap.put(key, (String) value);
			} else {
				throw new MLContextException("Incorrect type for input parameters");
			}
		}
		return convertedMap;
	}

	/**
	 * Convert input types to internal SystemML representations
	 * 
	 * @param parameterName
	 *            The name of the input parameter
	 * @param parameterValue
	 *            The value of the input parameter
	 * @return input in SystemML data representation
	 */
	public static Data convertInputType(String parameterName, Object parameterValue) {
		return convertInputType(parameterName, parameterValue, null);
	}

	/**
	 * Convert input types to internal SystemML representations
	 * 
	 * @param parameterName
	 *            The name of the input parameter
	 * @param parameterValue
	 *            The value of the input parameter
	 * @param matrixMetadata
	 *            matrix metadata
	 * @return input in SystemML data representation
	 */
	public static Data convertInputType(String parameterName, Object parameterValue, MatrixMetadata matrixMetadata) {
		String name = parameterName;
		Object value = parameterValue;
		if (name == null) {
			throw new MLContextException("Input parameter name is null");
		} else if (value == null) {
			throw new MLContextException("Input parameter value is null for: " + parameterName);
		} else if (value instanceof JavaRDD<?>) {
			@SuppressWarnings("unchecked")
			JavaRDD<String> javaRDD = (JavaRDD<String>) value;
			MatrixObject matrixObject;
			if ((matrixMetadata != null) && (matrixMetadata.getMatrixFormat() == MatrixFormat.IJV)) {
				matrixObject = MLContextConversionUtil.javaRDDStringIJVToMatrixObject(name, javaRDD, matrixMetadata);
			} else {
				matrixObject = MLContextConversionUtil.javaRDDStringCSVToMatrixObject(name, javaRDD, matrixMetadata);
			}
			return matrixObject;
		} else if (value instanceof RDD<?>) {
			@SuppressWarnings("unchecked")
			RDD<String> rdd = (RDD<String>) value;
			MatrixObject matrixObject;
			if ((matrixMetadata != null) && (matrixMetadata.getMatrixFormat() == MatrixFormat.IJV)) {
				matrixObject = MLContextConversionUtil.rddStringIJVToMatrixObject(name, rdd, matrixMetadata);
			} else {
				matrixObject = MLContextConversionUtil.rddStringCSVToMatrixObject(name, rdd, matrixMetadata);
			}

			return matrixObject;
		} else if (value instanceof MatrixBlock) {
			MatrixCharacteristics matrixCharacteristics;
			if (matrixMetadata != null) {
				matrixCharacteristics = matrixMetadata.asMatrixCharacteristics();
			} else {
				matrixCharacteristics = new MatrixCharacteristics();
			}
			MatrixFormatMetaData mtd = new MatrixFormatMetaData(matrixCharacteristics, OutputInfo.BinaryBlockOutputInfo, InputInfo.BinaryBlockInputInfo);
			MatrixObject matrixObject = new MatrixObject(ValueType.DOUBLE, MLContextUtil.scratchSpace() + "/" + name, mtd);
			try {
				matrixObject.acquireModify((MatrixBlock)value);
				matrixObject.release();
			} catch (CacheException e) {
				throw new MLContextException(e);
			}
			return matrixObject;
		}
		else if (value instanceof DataFrame) {
			DataFrame dataFrame = (DataFrame) value;
			MatrixObject matrixObject = MLContextConversionUtil
					.dataFrameToMatrixObject(name, dataFrame, matrixMetadata);
			return matrixObject;
		} else if (value instanceof BinaryBlockMatrix) {
			BinaryBlockMatrix binaryBlockMatrix = (BinaryBlockMatrix) value;
			if (matrixMetadata == null) {
				matrixMetadata = binaryBlockMatrix.getMatrixMetadata();
			}
			JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlocks = binaryBlockMatrix.getBinaryBlocks();
			MatrixObject matrixObject = MLContextConversionUtil.binaryBlocksToMatrixObject(name, binaryBlocks,
					matrixMetadata);
			return matrixObject;
		} else if (value instanceof Matrix) {
			Matrix matrix = (Matrix) value;
			MatrixObject matrixObject = matrix.asMatrixObject();
			return matrixObject;
		} else if (value instanceof double[][]) {
			double[][] doubleMatrix = (double[][]) value;
			MatrixObject matrixObject = MLContextConversionUtil.doubleMatrixToMatrixObject(name, doubleMatrix,
					matrixMetadata);
			return matrixObject;
		} else if (value instanceof Integer) {
			Integer i = (Integer) value;
			IntObject iObj = new IntObject(i);
			return iObj;
		} else if (value instanceof Double) {
			Double d = (Double) value;
			DoubleObject dObj = new DoubleObject(d);
			return dObj;
		} else if (value instanceof String) {
			String s = (String) value;
			StringObject sObj = new StringObject(s);
			return sObj;
		} else if (value instanceof Boolean) {
			Boolean b = (Boolean) value;
			BooleanObject bObj = new BooleanObject(b);
			return bObj;
		}
		return null;
	}

	/**
	 * Return the default matrix block size.
	 * 
	 * @return the default matrix block size
	 */
	public static int defaultBlockSize() {
		DMLConfig conf = ConfigurationManager.getDMLConfig();
		int blockSize = conf.getIntValue(DMLConfig.DEFAULT_BLOCK_SIZE);
		return blockSize;
	}

	/**
	 * Return the location of the scratch space directory.
	 * 
	 * @return the lcoation of the scratch space directory
	 */
	public static String scratchSpace() {
		DMLConfig conf = ConfigurationManager.getDMLConfig();
		String scratchSpace = conf.getTextValue(DMLConfig.SCRATCH_SPACE);
		return scratchSpace;
	}

	/**
	 * Return a double-quoted string with inner single and double quotes
	 * escaped.
	 * 
	 * @param str
	 *            the original string
	 * @return double-quoted string with inner single and double quotes escaped
	 */
	public static String quotedString(String str) {
		if (str == null) {
			return null;
		}

		StringBuilder sb = new StringBuilder();
		sb.append("\"");
		for (int i = 0; i < str.length(); i++) {
			char ch = str.charAt(i);
			if ((ch == '\'') || (ch == '"')) {
				if ((i > 0) && (str.charAt(i - 1) != '\\')) {
					sb.append('\\');
				} else if (i == 0) {
					sb.append('\\');
				}
			}
			sb.append(ch);
		}
		sb.append("\"");

		return sb.toString();
	}

	/**
	 * Display the keys and values in a Map
	 * 
	 * @param mapName
	 *            the name of the map
	 * @param map
	 *            Map of String keys and Object values
	 * @return the keys and values in the Map as a String
	 */
	public static String displayMap(String mapName, Map<String, Object> map) {
		StringBuilder sb = new StringBuilder();
		sb.append(mapName);
		sb.append(":\n");
		Set<String> keys = map.keySet();
		if (keys.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String key : keys) {
				sb.append("  [");
				sb.append(++count);
				sb.append("] ");
				sb.append(key);
				sb.append(": ");
				sb.append(map.get(key));
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * Display the values in a Set
	 * 
	 * @param setName
	 *            the name of the Set
	 * @param set
	 *            Set of String values
	 * @return the values in the Set as a String
	 */
	public static String displaySet(String setName, Set<String> set) {
		StringBuilder sb = new StringBuilder();
		sb.append(setName);
		sb.append(":\n");
		if (set.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String value : set) {
				sb.append("  [");
				sb.append(++count);
				sb.append("] ");
				sb.append(value);
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * Display the keys and values in the symbol table
	 * 
	 * @param name
	 *            the name of the symbol table
	 * @param symbolTable
	 *            the LocalVariableMap
	 * @return the keys and values in the symbol table as a String
	 */
	public static String displaySymbolTable(String name, LocalVariableMap symbolTable) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(":\n");
		sb.append(displaySymbolTable(symbolTable));
		return sb.toString();
	}

	/**
	 * Display the keys and values in the symbol table
	 * 
	 * @param symbolTable
	 *            the LocalVariableMap
	 * @return the keys and values in the symbol table as a String
	 */
	public static String displaySymbolTable(LocalVariableMap symbolTable) {
		StringBuilder sb = new StringBuilder();
		Set<String> keys = symbolTable.keySet();
		if (keys.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String key : keys) {
				sb.append("  [");
				sb.append(++count);
				sb.append("]");

				sb.append(" (");
				sb.append(determineOutputTypeAsString(symbolTable, key));
				sb.append(") ");

				sb.append(key);

				sb.append(": ");
				sb.append(symbolTable.get(key));
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * Obtain a symbol table output type as a String
	 * 
	 * @param symbolTable
	 *            the symbol table
	 * @param outputName
	 *            the name of the output variable
	 * @return the symbol table output type for a variable as a String
	 */
	public static String determineOutputTypeAsString(LocalVariableMap symbolTable, String outputName) {
		Data data = symbolTable.get(outputName);
		if (data instanceof BooleanObject) {
			return "Boolean";
		} else if (data instanceof DoubleObject) {
			return "Double";
		} else if (data instanceof IntObject) {
			return "Long";
		} else if (data instanceof StringObject) {
			return "String";
		} else if (data instanceof MatrixObject) {
			return "Matrix";
		} else if (data instanceof FrameObject) {
			return "Frame";
		}
		return "Unknown";
	}

	/**
	 * Obtain a display of script inputs.
	 * 
	 * @param name
	 *            the title to display for the inputs
	 * @param map
	 *            the map of inputs
	 * @return the script inputs represented as a String
	 */
	public static String displayInputs(String name, Map<String, Object> map) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(":\n");
		Set<String> keys = map.keySet();
		if (keys.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String key : keys) {
				Object object = map.get(key);
				@SuppressWarnings("rawtypes")
				Class clazz = object.getClass();
				String type = clazz.getSimpleName();
				if (object instanceof JavaRDD<?>) {
					type = "JavaRDD";
				} else if (object instanceof RDD<?>) {
					type = "RDD";
				}

				sb.append("  [");
				sb.append(++count);
				sb.append("]");

				sb.append(" (");
				sb.append(type);
				sb.append(") ");

				sb.append(key);
				sb.append(": ");
				String str = object.toString();
				str = StringUtils.abbreviate(str, 100);
				sb.append(str);
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * Obtain a display of the script outputs.
	 * 
	 * @param name
	 *            the title to display for the outputs
	 * @param outputNames
	 *            the names of the output variables
	 * @param symbolTable
	 *            the symbol table
	 * @return the script outputs represented as a String
	 * 
	 */
	public static String displayOutputs(String name, Set<String> outputNames, LocalVariableMap symbolTable) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(":\n");
		sb.append(displayOutputs(outputNames, symbolTable));
		return sb.toString();
	}

	/**
	 * Obtain a display of the script outputs.
	 * 
	 * @param outputNames
	 *            the names of the output variables
	 * @param symbolTable
	 *            the symbol table
	 * @return the script outputs represented as a String
	 * 
	 */
	public static String displayOutputs(Set<String> outputNames, LocalVariableMap symbolTable) {
		StringBuilder sb = new StringBuilder();
		if (outputNames.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String outputName : outputNames) {
				sb.append("  [");
				sb.append(++count);
				sb.append("] ");

				if (symbolTable.get(outputName) != null) {
					sb.append("(");
					sb.append(determineOutputTypeAsString(symbolTable, outputName));
					sb.append(") ");
				}

				sb.append(outputName);

				if (symbolTable.get(outputName) != null) {
					sb.append(": ");
					sb.append(symbolTable.get(outputName));
				}

				sb.append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * The SystemML welcome message
	 * 
	 * @return the SystemML welcome message
	 */
	public static String welcomeMessage() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nWelcome to Apache SystemML!\n");
		return sb.toString();
	}

	/**
	 * Generate a String history entry for a script.
	 * 
	 * @param script
	 *            the script
	 * @param when
	 *            when the script was executed
	 * @return a script history entry as a String
	 */
	public static String createHistoryForScript(Script script, long when) {
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
		StringBuilder sb = new StringBuilder();
		sb.append("Script Name: " + script.getName() + "\n");
		sb.append("When: " + dateFormat.format(new Date(when)) + "\n");
		sb.append(script.displayInputs());
		sb.append(script.displayOutputs());
		sb.append(script.displaySymbolTable());
		return sb.toString();
	}

	/**
	 * Generate a String listing of the script execution history.
	 * 
	 * @param scriptHistory
	 *            the list of script history entries
	 * @return the listing of the script execution history as a String
	 */
	public static String displayScriptHistory(List<String> scriptHistory) {
		StringBuilder sb = new StringBuilder();
		sb.append("MLContext Script History:\n");
		if (scriptHistory.isEmpty()) {
			sb.append("None");
		}
		int i = 1;
		for (String history : scriptHistory) {
			sb.append("--------------------------------------------\n");
			sb.append("#" + (i++) + ":\n");
			sb.append(history);
		}
		return sb.toString();
	}

}
