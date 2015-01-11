package nl.utwente.bigdata.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.collect.Maps;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Bolt which retrieves all tweets about referees
 * @TODO: implement
 * 
 * @author Djuri Baars
 * @author Martijn Hensema
 * @package Assignment7 
 */
public class GetRefereeTweetsBolt extends BaseBasicBolt {


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String tweet = (String) input.getValueByField("tweet");
		List<String> referees = (List<String>) input.getValueByField("referees");
		
		for(int i =0; i < referees.size(); i++)
	    {
	        if(tweet.contains(referees.get(i)))
	        {
	        	collector.emit(new Values(tweet, referees.get(i)));
	        }
	    }
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext) {		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tweet", "referee_name"));
	}
}
