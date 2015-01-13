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

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class NGramerBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 4031901444200770796L;
	private long M = 2;

	@Override
	public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {
		if (stormConf != null && stormConf.containsKey("M")) {
			M = Long.parseLong((String) stormConf.get("M"));
		}
	};
	
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String text = tuple.getString(0);
		text = text.replaceAll("[^A-Za-z0-9\\@\\#\\s]","");
		text = text.toLowerCase();
		String[] tokens = text.split("\\s+");
		for (int i = 0; i<tokens.length-M+1; i++) {
			StringBuilder builder = new StringBuilder();
			for (int j = 0; j < M; j++) {
				if (builder.length() > 0) builder.append(" ");
				builder.append(tokens[i+j]);
			}
			collector.emit(new Values(builder.toString()));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("n-gram"));
	}

}
