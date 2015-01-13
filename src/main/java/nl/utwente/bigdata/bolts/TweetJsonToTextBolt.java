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

import java.util.Map;

import org.json.simple.parser.JSONParser;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class TweetJsonToTextBolt extends BaseBasicBolt {
  private transient JSONParser parser;
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
	  parser = new JSONParser();
  }
  
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
	  try {
        Map<String, Object> tweet = (Map<String, Object>) parser.parse(tuple.getString(0));
        String text = (String) tweet.get("text");
        collector.emit(new Values(text));
      }
      catch (ClassCastException e) {  
    	System.out.println("ClassCass");
    	e.printStackTrace();
        return; // do nothing (we might log this)
      }
      catch (org.json.simple.parser.ParseException e) {
    	System.out.println("ParseException");
    	e.printStackTrace();
        return; // do nothing 
      } catch (Exception e) {
    	  e.printStackTrace();
      }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  declarer.declare(new Fields("words"));
  }

}
