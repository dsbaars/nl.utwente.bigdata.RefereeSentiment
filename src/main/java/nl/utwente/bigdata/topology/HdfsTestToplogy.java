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

package nl.utwente.bigdata.topology;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.utwente.bigdata.bolts.NormalizerBolt;
import nl.utwente.bigdata.bolts.PrinterBolt;
import nl.utwente.bigdata.spouts.TweetsHdfsSpout;
import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class HdfsTestToplogy extends AbstractTopologyRunner {   
	private Config conf;
	private static Logger logger = LoggerFactory.getLogger(HdfsTestToplogy.class);

	
	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();
		logger.info("HDFS TEST", properties.getProperty("worldcup-path"));
		
		
		conf = new Config();	
		conf.put("path", properties.getProperty("worldcup-path", "hdfs://127.0.0.1:8020/user/djuri/worldcup"));
		conf.put("hdfsConf", properties.getProperty("hdfs-xml-config", "/etc/hadoop/conf/core-site.xml"));
		
		builder.setSpout("hdfs", new TweetsHdfsSpout(conf));        
       
		builder.setBolt("normalizedTweets", new NormalizerBolt())
		.shuffleGrouping("hdfs"); 
		
		String boltId = "printer"; 
		builder.setBolt(boltId, new PrinterBolt()).shuffleGrouping("normalizedTweets"); 
		
		StormTopology topology = builder.createTopology();
		return topology;
		        
	}
	
    
    public static void main(String[] args) {
    	new HdfsTestToplogy().run(args);;
    }
}
