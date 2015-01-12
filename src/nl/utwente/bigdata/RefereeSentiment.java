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

import nl.utwente.bigdata.bolts.CalculateSentimentBolt;
import nl.utwente.bigdata.bolts.FileOutputBolt;
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
      
		
		String boltId = "";
		String spoutId = "";
		String prevId;
		
		// We define two streams here, one with tweets
		// the other with the world cup data
		spoutId = "tweets"; 
		builder.setSpout(spoutId, new TweetsJsonSpout()); 
		
//		spoutId = "worldcupJson"; 
//		builder.setSpout(spoutId, new WorldcupDataJsonSpout()); 

//		builder.setBolt("worldcupData", new WorldCupJsonToDataBolt()).shuffleGrouping("worldcupJson");
		
		// Tokenize referees
//		builder.setBolt("referees", new TokenizeRefereesBolt())
//			.shuffleGrouping("worldcupData"); 
		
		
//		
		// Get tweet texts
		builder.setBolt("tweetText", new TweetJsonToTextBolt()).shuffleGrouping("tweets"); 			
		
		// Tokenize referees
		builder.setBolt("normalizedTweets", new NormalizerBolt())
			.shuffleGrouping("tweetText"); 
//		
//		// First step: Extract tweets about referees 
		builder.setBolt("refereeTweets", new GetRefereeTweetsBolt())
			.shuffleGrouping("normalizedTweets")
		; 
//		prevId = boltId;
//		
//		// Then: Calculate Sentiment
//		boltId = "calculateSentiment"; 
//		builder.setBolt(boltId, new CalculateSentimentBolt()).shuffleGrouping("tweetText"); 
//		prevId = boltId;
//		
//		// Then: Link of games
//		builder.setBolt(boltId, new LinkToGameBolt()).shuffleGrouping(prevId); 

		prevId = "refereeTweets";
		boltId = "printer"; 
		builder.setBolt(boltId, new FileOutputBolt()).shuffleGrouping(prevId); 
		prevId = boltId;
		
		StormTopology topology = builder.createTopology();
		return topology;
		        
	}
	
    
    public static void main(String[] args) {
    	new RefereeSentiment().run(args);;
    }
}
