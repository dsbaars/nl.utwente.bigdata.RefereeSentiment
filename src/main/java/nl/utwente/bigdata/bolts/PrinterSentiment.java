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

import twitter4j.Status;
import nl.utwente.bigdata.utils.Emoji;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class PrinterSentiment extends BaseBasicBolt {
  private static final long serialVersionUID = 394263766896592119L;
  private String id;
  private String topology;
  private String language;

  public PrinterSentiment(Config conf) {
		this.language = (String) conf.get("language");
  }
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
	  this.id = context.getThisComponentId();
	  this.topology = context.getStormId();
  }
  
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {	
	  String icon;
	  
	  if ((Integer)tuple.getValueByField("sentiment") > 0) {
		  icon = ":-D";
	  } else if ((Integer)tuple.getValueByField("sentiment") < 0) {
		  icon = ">:[";
	  } else {
		  icon = ":-)";
	  }
	Status tweet = (Status)tuple.getValueByField("tweet");
	String tweetText = tweet.getText();
    System.out.println(Emoji.replaceFlagInText(this.language) 
    		+ " " +Emoji.replaceInText(icon) + " " 
    		+ "DATE: " + tweet.getCreatedAt().toGMTString() + " "
    		+ "MATCH: " + tuple.getValueByField("home") + "-" + tuple.getValueByField("away") + " - "
    		+ tweetText + " " + tuple.getValueByField("sentiment"));
    
    collector.emit(new Values(this.language, tuple.getStringByField("normalized_text"), tweet.getCreatedAt().toGMTString(), tuple.getStringByField("home"), tuple.getValueByField("away"), tuple.getValueByField("sentiment")));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
	  ofd.declare(new Fields("language", "normalized_text", "created_at", "home",  "away", "sentiment"));
  }

}
