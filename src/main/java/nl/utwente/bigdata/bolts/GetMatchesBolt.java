package nl.utwente.bigdata.bolts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.collect.Maps;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Bolt which retrieves all World Cup Matches
 * 
 * @TODO: implement
 * 
 * @author Djuri Baars
 * @author Martijn Hensema
 * @package Assignment7
 */
public class GetMatchesBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2627533197173873508L;
	private NavigableMap<Date, Pair<String,String>> matchesMap;
	public static final Logger logger = Logger.getLogger(GetMatchesBolt.class);  
	
	private OutputCollector _collector;
	@Override
	public void execute(Tuple input) {
		Status status = null;
		
		status =  (Status) input.getValueByField("tweet");
		
		//Date match = matchesMap.firstKey();
//		if (this.matchesMap != null) {
//			collector.emit(new Values(this.matchesMap.size()));
//		}
		logger.info(status.getCreatedAt() + "is search");
		Pair<String, String> match = this.matchesMap.get(this.matchesMap.lowerKey(status.getCreatedAt()));
		
		
		this._collector.emit(new Values(status, input.getValueByField("normalized_text"), input.getValueByField("sentiment"), match.getLeft(), match.getRight()));
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector collector) {
		this.matchesMap = new TreeMap<Date, Pair<String,String>>();;
		this._collector = collector;
		this.readMatchDates();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tweet", "normalized_text", "sentiment", "home", "away"));
	}


	protected void readMatchDates() {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				getClass().getClassLoader().getResourceAsStream(
						"wcdata/worldcup-matches.json")));
		StringBuilder sb = new StringBuilder();
		String line;
		try {
			while ((line = reader.readLine()) != null)
				sb.append(line);
		} catch (IOException e) {
			e.printStackTrace();
		}
		JSONParser parser = new JSONParser();		

		try {
			JSONArray matches = (JSONArray) parser.parse(sb.toString());
			for (Object match: matches) {
				JSONObject game = (JSONObject) match;
				JSONObject home = (JSONObject) game.get("home_team");
				JSONObject away = (JSONObject) game.get("away_team");
	
				String home_name = (String) home.get("country");
				String away_name = (String) away.get("country");
				// 2014-06-22T13:00:00.000-03:00
				// 2015-01-20T22:18:10.866-0300
				DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
				DateTime matchTime = new DateTime();
				matchTime = dtf.parseDateTime((String) game.get("datetime"));
				logger.info("Added match" + matchTime.toDate());
				this.matchesMap.put(matchTime.toDate(), Pair.of(home_name, away_name));
			}
		} catch (ClassCastException e) {
			System.out.println("ClassCass");
			e.printStackTrace();
			return; // do nothing (we might log this)
		} catch (ParseException e) {
			System.out.println("ParseException");
			e.printStackTrace();
			return; // do nothing
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("Total matches" + this.matchesMap.size());
	}
}
