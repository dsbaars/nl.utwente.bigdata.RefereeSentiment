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

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import nl.utwente.bigdata.bolts.FileOutputBolt;
import nl.utwente.bigdata.bolts.GetRefereeTweetsBolt;
import nl.utwente.bigdata.bolts.NormalizerBolt;
import nl.utwente.bigdata.bolts.PrinterBolt;
import nl.utwente.bigdata.bolts.TokenizerBolt;
import nl.utwente.bigdata.bolts.TweetJsonToTextBolt;
import nl.utwente.bigdata.spouts.JsonSpout;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class PrintKafkaTokensTopology extends AbstractTopologyRunner {   

    
	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();
		    
		SpoutConfig spoutConf = new SpoutConfig(new ZkHosts(properties.getProperty("zkhost", "ctit048.ewi.utwente.nl:2181")),
				  properties.getProperty("topic", "worldcup"), // topic to read from
				  "", // the root path in Zookeeper for the spout to store the consumer offsets
				  "worldcup");
		KafkaSpout spout = new KafkaSpout(spoutConf);
	
		String boltId = "";
		String prevId;
		
		boltId = "kafka"; 
		builder.setSpout(boltId, spout); 
		prevId = boltId;
		
		boltId = "tweetText"; 
		builder.setBolt(boltId, new TweetJsonToTextBolt()).shuffleGrouping(prevId); 
		prevId = boltId;
		
		//boltId = "tokenizer"; 
		//builder.setBolt(boltId, new TokenizerBolt()).shuffleGrouping(prevId);
		//prevId = boltId;
		
		//boltId = "printer"; 
		//builder.setBolt(boltId, new PrinterBolt()).shuffleGrouping(prevId); 
		//prevId = boltId;
		
		builder.setBolt("normalizedTweets", new NormalizerBolt()).shuffleGrouping("tweetText");
		builder.setBolt("refereeTweets", new GetRefereeTweetsBolt()).shuffleGrouping("normalizedTweets"); 
		
		prevId = "refereeTweets";
		boltId = "printer"; 
		builder.setBolt(boltId, new FileOutputBolt()).shuffleGrouping(prevId); 
		prevId = boltId;
		
		StormTopology topology = builder.createTopology();
		System.out.println(topology);
		return topology;
	}
	
    public static void main(String[] args) {
    	new PrintKafkaTokensTopology().run(args);;
    }
}
