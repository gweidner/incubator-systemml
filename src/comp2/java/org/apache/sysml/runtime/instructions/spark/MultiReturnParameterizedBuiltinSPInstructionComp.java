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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.sysml.runtime.functionobjects.KahanPlus;
import org.apache.sysml.runtime.instructions.cp.KahanObject;
import org.apache.sysml.runtime.matrix.data.FrameBlock;
import org.apache.sysml.runtime.matrix.data.FrameBlock.ColumnMetadata;
import org.apache.sysml.runtime.transform.MVImputeAgent;
import org.apache.sysml.runtime.transform.MVImputeAgent.MVMethod;
import org.apache.sysml.runtime.transform.RecodeAgent;
import org.apache.sysml.runtime.transform.encode.Encoder;
import org.apache.sysml.runtime.transform.encode.EncoderComposite;
import scala.Tuple2;


public class MultiReturnParameterizedBuiltinSPInstructionComp
{
	/**
	 * 
	 */
	public static class TransformEncodeBuildFunction implements PairFlatMapFunction<Iterator<Tuple2<Long, FrameBlock>>, Integer, String>
	{
		private static final long serialVersionUID = 6336375833412029279L;

		private Encoder _encoder = null;
		
		public TransformEncodeBuildFunction(Encoder encoder) {
			_encoder = encoder;
		}
		
		@Override
		public Iterator<Tuple2<Integer, String>> call(Iterator<Tuple2<Long, FrameBlock>> iter)
			throws Exception 
		{
			//build meta data (e.g., recode maps)
			while( iter.hasNext() ) {
				_encoder.build(iter.next()._2());	
			}
			
			//output recode maps as columnID - token pairs
			ArrayList<Tuple2<Integer,String>> ret = new ArrayList<Tuple2<Integer,String>>();
			if( _encoder instanceof EncoderComposite )
				for( Encoder cEncoder : ((EncoderComposite)_encoder).getEncoders() )
					if( cEncoder instanceof RecodeAgent ) {
						RecodeAgent ra = (RecodeAgent) cEncoder;
						HashMap<Integer,HashMap<String,Long>> tmp = ra.getCPRecodeMaps();
						for( Entry<Integer,HashMap<String,Long>> e1 : tmp.entrySet() )
							for( String token : e1.getValue().keySet() )
								ret.add(new Tuple2<Integer,String>(e1.getKey(), token));
					}
				
			return ret.iterator();
		}
	}
	
	/**
	 * 
	 */
	public static class TransformEncodeGroupFunction implements FlatMapFunction<Tuple2<Integer, Iterable<String>>, String>
	{
		private static final long serialVersionUID = -1034187226023517119L;

		private Accumulator<Long> _accMax = null;
		
		public TransformEncodeGroupFunction( Accumulator<Long> accMax ) {
			_accMax = accMax;
		}
		
		@Override
		public Iterator<String> call(Tuple2<Integer, Iterable<String>> arg0)
			throws Exception 
		{
			String colID = String.valueOf(arg0._1());
			Iterator<String> iter = arg0._2().iterator();
			
			ArrayList<String> ret = new ArrayList<String>();
			StringBuilder sb = new StringBuilder();
			long rowID = 1;
			while( iter.hasNext() ) {
				sb.append(rowID);
				sb.append(' ');
				sb.append(colID);
				sb.append(' ');
				sb.append(RecodeAgent.constructRecodeMapEntry(iter.next(), rowID));
				ret.add(sb.toString());
				sb.setLength(0); 
				rowID++;
			}
			_accMax.add(rowID-1);
			
			return ret.iterator();
		}
	}
	
	/**
	 * 
	 */
	public static class TransformEncodeBuild2Function implements PairFlatMapFunction<Iterator<Tuple2<Long, FrameBlock>>, Integer, ColumnMetadata>
	{
		private static final long serialVersionUID = 6336375833412029279L;

		private MVImputeAgent _encoder = null;
		
		public TransformEncodeBuild2Function(MVImputeAgent encoder) {
			_encoder = encoder;
		}
		
		@Override
		public Iterator<Tuple2<Integer, ColumnMetadata>> call(Iterator<Tuple2<Long, FrameBlock>> iter)
			throws Exception 
		{
			//build meta data (e.g., histograms and means)
			while( iter.hasNext() ) {
				FrameBlock block = iter.next()._2();
				_encoder.build(block);	
			}
			
			//extract meta data
			ArrayList<Tuple2<Integer,ColumnMetadata>> ret = new ArrayList<Tuple2<Integer,ColumnMetadata>>();
			int[] collist = _encoder.getColList();
			for( int j=0; j<collist.length; j++ ) {
				if( _encoder.getMethod(collist[j]) == MVMethod.GLOBAL_MODE ) {
					HashMap<String,Long> hist = _encoder.getHistogram(collist[j]);
					for( Entry<String,Long> e : hist.entrySet() )
						ret.add(new Tuple2<Integer,ColumnMetadata>(collist[j], 
								new ColumnMetadata(e.getValue(), e.getKey())));
				}
				else if( _encoder.getMethod(collist[j]) == MVMethod.GLOBAL_MEAN ) {
					ret.add(new Tuple2<Integer,ColumnMetadata>(collist[j], 
							new ColumnMetadata(_encoder.getNonMVCount(collist[j]), String.valueOf(_encoder.getMeans()[j]._sum))));
				}
				else if( _encoder.getMethod(collist[j]) == MVMethod.CONSTANT ) {
					ret.add(new Tuple2<Integer,ColumnMetadata>(collist[j],
							new ColumnMetadata(0, _encoder.getReplacement(collist[j]))));
				}
			}
			
			return ret.iterator();
		}
	}
	
	/**
	 * 
	 */
	public static class TransformEncodeGroup2Function implements FlatMapFunction<Tuple2<Integer, Iterable<ColumnMetadata>>, String>
	{
		private static final long serialVersionUID = 702100641492347459L;
		
		private MVImputeAgent _encoder = null;
		
		public TransformEncodeGroup2Function(MVImputeAgent encoder) {	
			_encoder = encoder;
		}

		@Override
		public Iterator<String> call(Tuple2<Integer, Iterable<ColumnMetadata>> arg0)
				throws Exception 
		{
			int colix = arg0._1();
			Iterator<ColumnMetadata> iter = arg0._2().iterator();
			ArrayList<String> ret = new ArrayList<String>();
			
			//compute global mode of categorical feature, i.e., value with highest frequency
			if( _encoder.getMethod(colix) == MVMethod.GLOBAL_MODE ) {
				HashMap<String, Long> hist = new HashMap<String,Long>();
				while( iter.hasNext() ) {
					ColumnMetadata cmeta = iter.next(); 
					Long tmp = hist.get(cmeta.getMvValue());
					hist.put(cmeta.getMvValue(), cmeta.getNumDistinct() + ((tmp!=null)?tmp:0));
				}
				long max = Long.MIN_VALUE; String mode = null;
				for( Entry<String, Long> e : hist.entrySet() ) 
					if( e.getValue() > max  ) {
						mode = e.getKey();
						max = e.getValue();
					}
				ret.add("-2 " + colix + " " + mode);
			}
			//compute global mean of categorical feature
			else if( _encoder.getMethod(colix) == MVMethod.GLOBAL_MEAN ) {
				KahanObject kbuff = new KahanObject(0, 0);
				KahanPlus kplus = KahanPlus.getKahanPlusFnObject();
				int count = 0;
				while( iter.hasNext() ) {
					ColumnMetadata cmeta = iter.next(); 
					kplus.execute2(kbuff, Double.parseDouble(cmeta.getMvValue()));
					count += cmeta.getNumDistinct();
				}
				if( count > 0 )
					ret.add("-2 " + colix + " " + String.valueOf(kbuff._sum/count));
			}
			//pass-through constant label
			else if( _encoder.getMethod(colix) == MVMethod.CONSTANT ) {
				if( iter.hasNext() )
					ret.add("-2 " + colix + " " + iter.next().getMvValue());
			}
			
			return ret.iterator();
		}
	}
}
