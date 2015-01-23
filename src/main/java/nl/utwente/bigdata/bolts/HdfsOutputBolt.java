/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.utwente.bigdata.bolts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class HdfsOutputBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private File f;
	
	
	@Override
	public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {
		try {
			f = File.createTempFile(context.getStormId(), "txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	};
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		OutputStream fstream;
		//final String[] fields = "tweet", "normalized_text", "sentiment", "home", "away"
		
		try {
			fstream = new FileOutputStream(f, true);
			PrintStream pw = new PrintStream(fstream);
			pw.println(tuple);
			pw.close();
			fstream.close();
			System.out.println("Wrote to " + f.getAbsolutePath());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}
