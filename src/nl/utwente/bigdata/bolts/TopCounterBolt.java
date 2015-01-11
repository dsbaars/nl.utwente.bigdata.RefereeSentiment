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

import java.util.ArrayList;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.utwente.bigdata.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TopCounterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private Map<Object, Integer> counts = new HashMap<Object, Integer>();
	private long N = 3;
	private long generation = 0;
	private long cycle = 10;

	private static final class ValueComparator<V extends Comparable<? super V>>
                                     implements Comparator<Map.Entry<?, V>> {
		public int compare(Map.Entry<?, V> o1, Map.Entry<?, V> o2) {
			return -1 * o1.getValue().compareTo(o2.getValue());
		}
	}
	


	@Override
	public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {
		if (stormConf != null && stormConf.containsKey("N")) {
			N = Long.parseLong((String) stormConf.get("N"));
		}
		if (stormConf != null && stormConf.containsKey("CYCLE")) {
			cycle = Long.parseLong((String) stormConf.get("CYCLE"));
			System.out.println("!!!!!!!!!!Cycle " + cycle);
		}
	};
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (TupleHelpers.isTickTuple(tuple)) {
			// this was a chronological tickle tuple - generate output
			final int size = counts.size();
		    final List<Map.Entry<Object, Integer>> list = new ArrayList<Map.Entry<Object, Integer>>(size);
		    list.addAll(counts.entrySet());
		    final ValueComparator<Integer> cmp = new ValueComparator<Integer>();
		    Collections.sort(list, cmp);
		    for (int i = 0; i < Math.min(N, size); i++) {
		    	Map.Entry<Object, Integer> e = list.get(i);
		    	collector.emit(new Values(e.getKey(), e.getValue(), generation)); 
		    }
		    generation++;
		    
			counts.clear();
		} else {
			// this was a real tuple - add to input
			Object value = tuple.getValue(0);
			Integer count = counts.get(value);
			if (count == null) count = 0;
			counts.put(value, count+1);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("obj", "count", "generation"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, cycle);
		return conf;
	}
}
