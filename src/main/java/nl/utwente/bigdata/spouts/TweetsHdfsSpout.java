package nl.utwente.bigdata.spouts;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.List;

import nl.utwente.bigdata.utils.NaturalOrderComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

/**
 * Spout which reads all WordCup files from HDFS
 * 
 * @author Djuri Baars
 * @author Martijn Hensema
 * @package Assignment7
 */
public class TweetsHdfsSpout extends BaseRichSpout {
	private static final long serialVersionUID = -4086738198815956458L;
	private static Logger logger = LoggerFactory
			.getLogger(TweetsHdfsSpout.class);
	private SpoutOutputCollector _collector;
	private String path;
	private String hdfsConf = "/etc/hadoop/conf/core-site.xml";
	Queue<Status> queue = null;

	public TweetsHdfsSpout(Config conf) {
		this.path = (String) conf.get("path");
		this.hdfsConf = (String) conf.get("hdfsConf");
	}

	public TweetsHdfsSpout(String path) {
		this.path = path;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.queue = new LinkedList<Status>();
		this.logger.info("Opening HDFS config" + this.hdfsConf);
		this.logger.info("Opening folder" + this.path);
		
		this._collector = collector;
		

			new Thread(new Runnable() {
	            @Override
	            public void run() {
	            	try {
            		// pickup config files off classpath
            		Configuration hdfsConfig = new Configuration();
            		// explicitely add other config files
            		// PASS A PATH NOT A STRING!
            		hdfsConfig.addResource(new Path(hdfsConf));
	            		
	            	FileSystem fs = FileSystem.get(hdfsConfig);

	    			FileStatus[] String = null;

	    			String = fs.listStatus(new Path(path));

	    			List<FileStatus> StringList = new ArrayList<FileStatus>(
	    					Arrays.asList(String));

	    			Collections.sort(StringList, new NaturalOrderComparator());

	    				logger.info("Opening HDFS");
	    			
	                	for (FileStatus s : StringList) {

	        				InputStreamReader isr = new InputStreamReader(fs.open(s.getPath()));

	        				BufferedReader br = new BufferedReader(isr, 1);
	        				logger.info("Reading HDFS..." + s.getPath().toString());

	        				String line;
	        				line = br.readLine();
	        				while (line != null) {
	        					queue.offer(TwitterObjectFactory.createStatus(line));

	        					line = br.readLine();
	        				}

	        		
	        			}
	            	} catch (Exception e1) {
	        			// TODO Auto-generated catch block
	        			e1.printStackTrace();
	        			System.exit(1);
	        		}
	            	
	            
	         }
	}).start();
			
		

		

	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			_collector.emit(new Values(ret));

		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}