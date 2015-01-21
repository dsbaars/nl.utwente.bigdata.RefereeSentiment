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
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import twitter4j.Status;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Split to multiple language streams
 * @TODO: implement
 * 
 * @author Djuri Baars
 * @author Martijn Hensema
 * @package Assignment7 
 */
public class FilterLanguageBolt extends BaseBasicBolt {
	public static final Logger logger = Logger.getLogger(FilterLanguageBolt.class);  
	private static final long serialVersionUID = -8526320899331924698L;
	private final String[]languages = new String[]{"en", "fr", "es", "it", "de", "nl"};

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
	  
  }
  
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
		String lang = tuple.getStringByField("lang");
		Status status =  (Status) tuple.getValueByField("tweet");
		
		if (Arrays.asList(this.languages).contains(status.getLang())) {
			collector.emit(status.getLang(), tuple.getValues());
		} else {
			//logger.info(lang + " not found");
		}

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  for (String lang: this.languages) {
		  declarer.declareStream(lang, new Fields("tweet", "normalized_text", "lang"));
		  logger.info("Filter language prepared for " + lang);
	  }
  }

}
