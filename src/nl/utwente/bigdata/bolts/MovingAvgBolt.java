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

public class MovingAvgBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private static final int W = 5;
	private double[] window = new double[W];
	private double sum = 0.0;
	private int i = 0;
	
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO: add here the functionality that accumulates the W values. 
		// Once there are enough values, every received tuple also causes 
		// the emission of the average over the last W values;
		Double val = tuple.getDouble(0);
		int pos = i % W;
		sum -= window[pos];
		sum += val;
		window[pos] = val;
		i++;
		if (i >= W) collector.emit(new Values(sum / W));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("average"));
	}
}
