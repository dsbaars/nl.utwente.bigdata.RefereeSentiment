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
import nl.utwente.bigdata.bolts.TokenizeRefereesBolt;
import nl.utwente.bigdata.bolts.TokenizerBolt;
import nl.utwente.bigdata.bolts.TweetJsonToTextBolt;
import nl.utwente.bigdata.bolts.WorldCupJsonToDataBolt;
import nl.utwente.bigdata.spouts.TweetsJsonSpout;
import nl.utwente.bigdata.spouts.WorldcupDataJsonSpout;
import nl.utwente.bigdata.spouts.TwitterSpout;
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
	
	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();
      
		SpoutConfig kafkaConf = new SpoutConfig(new ZkHosts(properties.getProperty("zkhost", "130.89.171.23:2181")),
				  "worldcup", // topic to read from
				  "/brokers", // the root path in Zookeeper for the spout to store the consumer offsets
				  "default");
		
		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//		kafkaConf.startOffsetTime = -2;
//		kafkaConf.forceFromStart = true;
		builder.setSpout("tweets", new KafkaSpout(kafkaConf), 1);
		
		String boltId = "";
		String spoutId = "";
		String prevId;
		
		// We define two streams here, one with tweets
		// the other with the world cup data
//		spoutId = "tweets"; 
//		builder.setSpout(spoutId, new TweetsJsonSpout()); 
		
//		spoutId = "worldcupJson"; 
//		builder.setSpout(spoutId, new WorldcupDataJsonSpout()); 

//		builder.setBolt("worldcupData", new WorldCupJsonToDataBolt()).shuffleGrouping("worldcupJson");
		
		// Tokenize referees
//		builder.setBolt("referees", new TokenizeRefereesBolt())
//			.shuffleGrouping("worldcupData"); 
		
		
//		
		// Get tweet texts
		builder.setBolt("tweetText", new TweetJsonToTextBolt()).shuffleGrouping("tweets"); 			
		
		builder.setBolt("dutchTweets", new FilterLanguageBolt())
			.shuffleGrouping("tweetText")
		; 
		
		// Tokenize referees
		builder.setBolt("normalizedTweets", new NormalizerBolt())
			.shuffleGrouping("dutchTweets"); 
		
		
//		
//		// First step: Extract tweets about referees 
		builder.setBolt("refereeTweets", new GetRefereeTweetsBolt())
			.shuffleGrouping("normalizedTweets")
		; 
//		prevId = boltId;
//		
		// Then: Calculate Sentiment
		boltId = "calculateSentiment"; 
		builder.setBolt(boltId, new CalculateSentimentBolt()).shuffleGrouping("refereeTweets"); 
		prevId = boltId;
//		
//		// Then: Link of games
//		builder.setBolt(boltId, new LinkToGameBolt()).shuffleGrouping(prevId); 

		prevId = "calculateSentiment";
		boltId = "printer"; 
		builder.setBolt(boltId, new PrinterBolt()).shuffleGrouping(prevId); 
		prevId = boltId;
		
		StormTopology topology = builder.createTopology();
		return topology;
		        
	}
	
    
    public static void main(String[] args) {
    	new RefereeSentiment().run(args);;
    }
}
