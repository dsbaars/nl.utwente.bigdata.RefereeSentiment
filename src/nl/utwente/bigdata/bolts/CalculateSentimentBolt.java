package nl.utwente.bigdata.bolts;

import java.util.Map;
import java.util.SortedMap;

import com.google.common.collect.Maps;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Calculates sentiment of tweet
 * @TODO: implement
 * 
 * @author Djuri Baars
 * @author Martijn Hensema
 */
public class CalculateSentimentBolt extends BaseRichBolt {

	private static final long serialVersionUID = -8884479245628635300L;
	
	private SortedMap<String, Integer> sentimentMap = null;

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.sentimentMap = Maps.newTreeMap();
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub

	}

	/**
	 * Actual calculation method
	 * @return
	 */
	protected final int calculateSentiment(Status message) {
		int calculatedSentiment = 0;
		
		return calculatedSentiment;
	}
}
