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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

import twitter4j.Status;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class FileOutputBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private File f;
	final String[] fields = new String[]{"tweet", "normalized_text", "sentiment", "home", "away"};
	private BufferedWriter fstream;
	private PrintWriter pw;
	private BufferedWriter writer;
	public static final Logger logger = Logger.getLogger(FileOutputBolt.class);  

	@Override
	public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {
		try {			
			this.writer = new BufferedWriter(new FileWriter("/tmp/bigdata.csv"));
			this.pw = new PrintWriter(this.writer);
		//	logger.info("Wrote to " + this.f.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
		}
	};
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {	
		Status tweet = (Status) tuple.getValueByField("tweet");

		this.pw.println(String.format("%s;%s;%s;%s;%s;%s", 
				tweet.getLang(),
				tweet.getCreatedAt().toGMTString(),
				tuple.getStringByField("normalized_text"),
				tuple.getIntegerByField("sentiment"),
				tuple.getStringByField("home"),
				tuple.getStringByField("away")
				));
	//	logger.info("Wrote to " + this.f.getAbsolutePath());
		try {
			this.writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}
