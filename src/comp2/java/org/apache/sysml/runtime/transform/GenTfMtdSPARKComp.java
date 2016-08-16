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

package org.apache.sysml.runtime.transform;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import scala.Tuple2;

import org.apache.sysml.runtime.matrix.CSVReblockMR.OffsetCount;

public class GenTfMtdSPARKComp 
{
	// ------------------------------------------------------------------------------------------------
	
	/*private*/ static class GenTfMtdReduce implements FlatMapFunction<Tuple2<Integer, Iterable<DistinctValue>>, Long> 
	{	
		private static final long serialVersionUID = -2733233671193035242L;
		private TfUtils _agents = null;
		
		public GenTfMtdReduce(boolean hasHeader, String delim, String naStrings, String headerLine, String tfMtdDir, String offsetFile, String spec, long numCols) throws IOException, JSONException {
			String[] nas = TfUtils.parseNAStrings(naStrings); 
			JSONObject jspec = new JSONObject(spec);
			_agents = new TfUtils(headerLine, hasHeader, delim, nas, jspec, numCols, tfMtdDir, offsetFile, null);
		}

		@SuppressWarnings("unchecked")
		@Override
		public Iterator<Long> call(Tuple2<Integer, Iterable<DistinctValue>> t)
				throws Exception {
			
			int colID = t._1();
			Iterator<DistinctValue> iterDV = t._2().iterator();

			JobConf job = new JobConf();
			FileSystem fs = FileSystem.get(job);
			
			ArrayList<Long> numRows = new ArrayList<Long>();
			
			if(colID < 0) 
			{
				// process mapper output for MV and Bin agents
				colID = colID*-1;
				_agents.getMVImputeAgent().mergeAndOutputTransformationMetadata(iterDV, _agents.getTfMtdDir(), colID, fs, _agents);
				numRows.add(0L);
			}
			else if ( colID == _agents.getNumCols() + 1)
			{
				// process mapper output for OFFSET_FILE
				ArrayList<OffsetCount> list = new ArrayList<OffsetCount>();
				while(iterDV.hasNext())
					list.add(new OffsetCount(iterDV.next().getOffsetCount()));
				Collections.sort(list);
				
				@SuppressWarnings("deprecation")
				SequenceFile.Writer writer = new SequenceFile.Writer(fs, job, new Path(_agents.getOffsetFile()+"/part-00000"), ByteWritable.class, OffsetCount.class);
				
				long lineOffset=0;
				for(OffsetCount oc: list)
				{
					long count=oc.count;
					oc.count=lineOffset;
					writer.append(new ByteWritable((byte)0), oc);
					lineOffset+=count;
				}
				writer.close();
				list.clear();
				
				numRows.add(lineOffset);
			}
			else 
			{
				// process mapper output for Recode agent
				_agents.getRecodeAgent().mergeAndOutputTransformationMetadata(iterDV, _agents.getTfMtdDir(), colID, fs, _agents);
				numRows.add(0L);
			}
			
			return numRows.iterator();
		}
	}
}
