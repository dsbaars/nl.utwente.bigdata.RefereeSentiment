package nl.utwente.bigdata.bolts;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import twitter4j.Status;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Calculates sentiment of tweet
 * @TODO: implement
 * 
 * @author Djuri Baars
 * @author Martijn Hensema
 * @package Assignment7 
 */
public class CalculateSentimentBolt extends BaseRichBolt {

	private static final long serialVersionUID = -8884479245628635300L;
	private OutputCollector _collector;
	private SortedMap<String, Integer> sentimentMap = null;
	private String language;
	
	
	
	public CalculateSentimentBolt(Config conf) {
		this.language = (String) conf.get("language");
	}

	@Override
	public void execute(Tuple input) {
		Status tweet = (Status) input.getValueByField("tweet");
		String normalized_text = ((String) input.getStringByField("normalized_text")).toLowerCase();
		
		final int sentimentCurrentTweet = calculateSentiment(normalized_text);
		
		this._collector.emit(new Values(tweet, normalized_text, sentimentCurrentTweet));
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.sentimentMap = Maps.newTreeMap();
		
		this._collector = collector;
		
		final String sentimentFilePath = "lang/" + this.language + "/sentiment.txt";

		//Bolt will read the AFINN Sentiment file [which is in the classpath] and stores the key, value pairs to a Map.
		try {
			final URL url = getClass().getClassLoader().getResource(
					sentimentFilePath);
			final String text = Resources.toString(url, Charsets.UTF_8);
			final Iterable<String> lineSplit = Splitter.on("\n").trimResults().omitEmptyStrings().split(text);
			List<String> tabSplit;
			for (final String str: lineSplit) {
				tabSplit = Lists.newArrayList(Splitter.on("\t").trimResults().omitEmptyStrings().split(str));
				
				int lastRow = tabSplit.size()-1;
				Integer sentimentScore =  Integer.parseInt(tabSplit.get(lastRow));
				sentimentMap.put(tabSplit.get(0), sentimentScore);
			}
		} catch (final IOException e) {
			e.printStackTrace();
			//Should not occur. If it occurs, we cant continue. So, exiting at this point itself.
			System.exit(1);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tweet", "normalized_text", "sentiment"));
	}

	/**
	 * Actual calculation method
	 * @return
	 */
	protected final int calculateSentiment(String tweet) {
		final Iterable<String> words = Splitter.on(' ')
                .trimResults()
                .omitEmptyStrings()
                .split(tweet);
		int calculatedSentiment = 0;
		for (final String word : words) {
			if(sentimentMap.containsKey(word)){
				calculatedSentiment += this.sentimentMap.get(word);
			}
		}
		return calculatedSentiment;
	}
}
