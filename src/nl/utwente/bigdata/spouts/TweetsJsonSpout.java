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
package nl.utwente.bigdata.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.yaml.snakeyaml.reader.StreamReader;

public class TweetsJsonSpout extends BaseRichSpout {
  private static final long serialVersionUID = -1497360044271864620L;
  SpoutOutputCollector _collector;
  Random _rand;
  List<String> sentences = new ArrayList<String>(); 
  boolean emitted = false;
  
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {	
    try {
    	System.out.println("Reading tweets");
    	BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("tweets.json")));

		String line = null;
		while ((line = reader.readLine()) != null) {
			sentences.add(line);
		}
		reader.close();
		System.out.println("Reading done");
	} catch (IOException e) {
		e.printStackTrace();
	} catch (Exception e) {
		e.printStackTrace();
	}
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    if (!emitted &&  sentences.size() > 0) {
	    for (int i = 0; i < sentences.size(); i++) {
	        _collector.emit(new Values(sentences.get(i)));
	    }
	    emitted = true;
    }
    
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tweet"));
  }

}