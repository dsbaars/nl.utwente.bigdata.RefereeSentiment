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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import com.google.common.collect.ImmutableList;

import scala.collection.immutable.List;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import nl.utwente.bigdata.bolts.PrinterBolt;
import nl.utwente.bigdata.bolts.TokenizerBolt;
import nl.utwente.bigdata.bolts.TweetJsonToTextBolt;
import nl.utwente.bigdata.hdfs.FileIOCommandBolt;
import nl.utwente.bigdata.hdfs.IOShellCommand;
import nl.utwente.bigdata.spouts.TweetsHdfsSpout;
import nl.utwente.bigdata.spouts.WorldcupDataJsonSpout;
import nl.utwente.bigdata.spouts.TwitterSpout;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class HdfsTestToplogy extends AbstractTopologyRunner {   

	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();

		
		builder.setSpout("hdfs", new TweetsHdfsSpout());        
       
		
		String boltId = "printer"; 
		builder.setBolt(boltId, new PrinterBolt()).shuffleGrouping("hdfs"); 
		
		StormTopology topology = builder.createTopology();
		return topology;
		        
	}
	
    
    public static void main(String[] args) {
    	new HdfsTestToplogy().run(args);;
    }
}
