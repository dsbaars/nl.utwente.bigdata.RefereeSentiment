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

package nl.utwente.bigdata;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.hdfs.bolt.HdfsBolt;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import nl.utwente.bigdata.bolts.CalculateSentimentBolt;
import nl.utwente.bigdata.bolts.FileOutputBolt;
import nl.utwente.bigdata.bolts.FilterLanguageBolt;
import nl.utwente.bigdata.bolts.GetRefereeTweetsBolt;
import nl.utwente.bigdata.bolts.LinkToGameBolt;
import nl.utwente.bigdata.bolts.NormalizerBolt;
import nl.utwente.bigdata.bolts.PrinterBolt;
import nl.utwente.bigdata.bolts.PrinterSentiment;
import nl.utwente.bigdata.bolts.TokenizeRefereesBolt;
import nl.utwente.bigdata.bolts.TokenizerBolt;
import nl.utwente.bigdata.bolts.TweetJsonToTextBolt;
import nl.utwente.bigdata.bolts.WorldCupJsonToDataBolt;
import nl.utwente.bigdata.spouts.TweetsJsonSpout;
import nl.utwente.bigdata.spouts.WorldcupDataJsonSpout;
import nl.utwente.bigdata.spouts.TwitterSpout;
import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**	
 * @author Djuri Baars
 * @author Martijn Hensema
 * @package Assignment7 
 */
public class RefereeSentiment extends AbstractTopologyRunner {   
	final String[] languages = new String[]{ "en", "es", "fr", "it", "de", "nl"};
	public static final Logger logger = Logger.getLogger(RefereeSentiment.class);  
	
	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();
		String boltId = "";
		String spoutId = "";
		String prevId;
		
		SpoutConfig kafkaConf = new SpoutConfig(new ZkHosts(properties.getProperty("zkhost", "130.89.171.23:2181")),
				  "worldcup", // topic to read from
				  "/brokers", // the root path in Zookeeper for the spout to store the consumer offsets
				  "worldcup");
		
		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//		kafkaConf.startOffsetTime = -2;
//		kafkaConf.forceFromStart = true;
		builder.setSpout("tweets", new KafkaSpout(kafkaConf), 1);
	
		// Get tweet texts
		builder.setBolt("tweetText", new TweetJsonToTextBolt())
			.shuffleGrouping("tweets"); 	
	
		builder.setBolt("normalizedTweets", new NormalizerBolt())
			.shuffleGrouping("tweetText"); 
		
		builder.setBolt("filteredLanguages", new FilterLanguageBolt())
			.shuffleGrouping("normalizedTweets")
		; 
		
		final String getRefereeBoltName = "%s_getReferee";
		final String calculateSentimentBoltName = "%s_calculateSentiment";
		final String printerBoltName = "%s_printer";
		
		for (String lang: this.languages) {
			Config conf = new Config();
			conf.put("language", lang);
			
			logger.info("Preparing for language " + lang);
			
			// First step: Extract tweets about referees 
			builder.setBolt(String.format(getRefereeBoltName, lang), new GetRefereeTweetsBolt(conf))
				.shuffleGrouping("filteredLanguages", lang)
			;
			
			// Then: Calculate Sentiment
			builder.setBolt(String.format(calculateSentimentBoltName, lang), new CalculateSentimentBolt(conf))
				.shuffleGrouping(String.format(getRefereeBoltName, lang))
			;
			
			// Each language gets a printer ...for now
			builder.setBolt(String.format(printerBoltName, lang), new PrinterSentiment(conf))
				.shuffleGrouping(String.format(calculateSentimentBoltName, lang))
			; 
		}
				
		StormTopology topology = builder.createTopology();
		return topology;
		        
	}
	
    
    public static void main(String[] args) {
    	new RefereeSentiment().run(args);;
    }
}
