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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

public class WorldcupDataJsonSpout extends BaseRichSpout {
  private static final long serialVersionUID = -1497360044271864620L;
  SpoutOutputCollector _collector;
  Random _rand;
  List<String> matches = new ArrayList<String>(); 

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {	
    try {
    	System.out.println("Reading worldcup-data");
    	BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("worldcup-games.json")));
    	StringBuilder sb = new StringBuilder();
    	String line;
		while ((line = reader.readLine()) != null) sb.append(line);
		JSONParser parser=new JSONParser();
		JSONArray collection = (JSONArray) parser.parse(sb.toString());
		Iterator iter = collection.iterator();
		while(iter.hasNext()){
				JSONObject entry = (JSONObject)iter.next();
		      matches.add(JSONValue.toJSONString( entry ));
		  //    System.out.println(JSONValue.toJSONString( entry ));
		    }
		reader.close();
		System.out.println("Reading done");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (Exception e) {
		e.printStackTrace();
	}
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
   // Utils.sleep(100);
    String match = matches.get(_rand.nextInt(matches.size()));
    _collector.emit(new Values(match));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("match"));
  }

}