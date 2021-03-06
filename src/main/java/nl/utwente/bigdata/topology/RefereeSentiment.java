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

import org.apache.log4j.Logger;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import nl.utwente.bigdata.bolts.CalculateSentimentBolt;
import nl.utwente.bigdata.bolts.FileOutputBolt;
import nl.utwente.bigdata.bolts.FilterLanguageBolt;
import nl.utwente.bigdata.bolts.GetMatchesBolt;
import nl.utwente.bigdata.bolts.GetRefereeTweetsBolt;
import nl.utwente.bigdata.bolts.NormalizerBolt;
import nl.utwente.bigdata.bolts.PrinterSentiment;
import nl.utwente.bigdata.bolts.TweetJsonToTextBolt;
import nl.utwente.bigdata.spouts.TweetsHdfsSpout;
import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

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
		Config hdfsConf = new Config();
		
//		builder.setSpout(new HdfsSpout(), spout)
//		
//		SpoutConfig kafkaConf = new SpoutConfig(new ZkHosts(properties.getProperty("zkhost", "ctit048.ewi.utwente.nl:2181")),
//				  "worldcup_real", // topic to read from
//				  "/brokers", // the root path in Zookeeper for the spout to store the consumer offsets
//				  "worldcup");
////		
//		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//		kafkaConf.startOffsetTime = -2;
////		kafkaConf.forceFromStart = true;
//		builder.setSpout("tweets", new KafkaSpout(kafkaConf), 1);
		hdfsConf.put("path", properties.getProperty("worldcup-path", "hdfs://127.0.0.1:8020/user/djuri/worldcup"));
		hdfsConf.put("hdfsConf", properties.getProperty("hdfs-xml-config", "/etc/hadoop/conf/core-site.xml"));
		
		builder.setSpout("tweets", new TweetsHdfsSpout(hdfsConf));        
//		builder.setBolt("tweets", new TweetJsonToTextBolt())
//			.shuffleGrouping("tweetsText"); 
	
		builder.setBolt("normalizedTweets", new NormalizerBolt())
			.shuffleGrouping("tweets"); 
		
		builder.setBolt("filteredLanguages", new FilterLanguageBolt())
			.shuffleGrouping("normalizedTweets")
		; 
		
		final String getRefereeBoltName = "%s_getReferee";
		final String calculateSentimentBoltName = "%s_calculateSentiment";
		final String getMatchesBoltName = "%s_matches";
		final String printerBoltName = "%s_printer";
		final String fileOutputBoltName = "%s_file_output";
		
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
		RecordFormat format = new DelimitedRecordFormat()
			.withFieldDelimiter(";");
		
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
			
			// Then: Append Match 
			builder.setBolt(String.format(getMatchesBoltName, lang), new GetMatchesBolt())
				.shuffleGrouping(String.format(calculateSentimentBoltName, lang))
			;
			
			builder.setBolt(String.format(printerBoltName, lang), new PrinterSentiment(conf))
				.shuffleGrouping(String.format(getMatchesBoltName, lang))
			; 
			
			// Each language gets a printer ...for now
			builder.setBolt(String.format(fileOutputBoltName, lang), new FileOutputBolt(conf))
				.shuffleGrouping(String.format(getMatchesBoltName, lang))
			; 
			
			
//			FileNameFormat fileNameFormat = new DefaultFileNameFormat()
//				.withPath(String.format("/user/djuri/s1017497-referee-sentiment-real-%s/", lang));
//			
//			HdfsBolt hdfsBolt = new HdfsBolt()
//		        .withFsUrl("hdfs://studyserver2:8020")
//		        .withFileNameFormat(fileNameFormat)
//		        .withRecordFormat(format)
//		        .withRotationPolicy(rotationPolicy)
//		        .withSyncPolicy(syncPolicy);
//			builder.setBolt(String.format(fileOutputBoltName, lang), hdfsBolt)
//				.shuffleGrouping(String.format(printerBoltName, lang))
//			; 
			
		}
				
		StormTopology topology = builder.createTopology();
		return topology;
		        
	}
	
    
    public static void main(String[] args) {
    	new RefereeSentiment().run(args);;
    }
}
