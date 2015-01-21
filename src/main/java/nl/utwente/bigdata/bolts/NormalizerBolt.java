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

import java.text.Normalizer;
import java.util.Map;
import java.util.regex.Pattern;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class NormalizerBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -8526320899331924698L;
	private String field;
	private JSONParser parser;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
	  //this.field = "words";
	  this.parser = new JSONParser();
  }
  
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
	  	Status tweet;
		tweet = (Status) tuple.getValueByField("tweet");

		// from: http://stackoverflow.com/questions/1008802/converting-symbols-accent-letters-to-english-alphabet
		Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
		String nfdNormalizedString = "";	
		nfdNormalizedString = Normalizer.normalize(tweet.getText(), Normalizer.Form.NFD); 
		
		String normalizedTweet = (String)pattern.matcher(nfdNormalizedString.toLowerCase()).replaceAll("");
		// Also remove prefixed with rt
		if (!normalizedTweet.startsWith("rt")) {
			collector.emit(new Values(tweet, normalizedTweet, tweet.getLang()));
		}
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  declarer.declare(new Fields("tweet", "normalized_text", "lang"));
  }

}
