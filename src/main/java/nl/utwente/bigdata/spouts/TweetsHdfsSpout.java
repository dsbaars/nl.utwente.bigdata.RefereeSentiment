package nl.utwente.bigdata.spouts;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class TweetsHdfsSpout  extends BaseRichSpout {
	private static Logger logger = LoggerFactory.getLogger(TweetsHdfsSpout.class);
	private List<File> files = Lists.newLinkedList();
	private Set<File> workingSet = new HashSet<File>();
	private SpoutOutputCollector _collector;
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector = collector;
		 // pickup config files off classpath
		 Configuration hdfsConf = new Configuration();
		 // explicitely add other config files
		 // PASS A PATH NOT A STRING!
		 hdfsConf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		 
		try {
			FileSystem fs = FileSystem.get(hdfsConf);
			FileStatus[] status = fs.listStatus(new Path("hdfs://127.0.0.1:8020/user/djuri/worldcup"));
            this.logger.info("Opening HDFS");

			for (FileStatus s: status){
             //   this.logger.info(s.getPath().toString());
                this.files.add(new File(s.getPath().toUri().getPath()));
			}
		}
		catch (Exception e) {
			this.logger.info(e.getMessage());
		}
//		Object projectPath = conf.get("project_dir");
//		if (projectPath != null)
//			projectDir = new File((String) projectPath);
//		else {
//			LOG.error("project path is not provided");
//			throw new RuntimeException("Project dir is not provided");
//		}
	}

	@Override
	public void nextTuple() {
		try {
			// check if new files exist
			for (File file : this.files) {
				if (!workingSet.contains(file)) {
					_collector.emit(new Values(file
							.getAbsolutePath()), file.getAbsoluteFile());
					workingSet.add(file);
				}
			}
			Thread.sleep(100);
		} catch (InterruptedException e) {
			this.logger.error(e.getMessage());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("path"));
	}

}