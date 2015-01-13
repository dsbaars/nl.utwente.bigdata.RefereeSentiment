package nl.utwente.bigdata.bolts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
 * 
 * @TODO: implement
 * 
 * @author Djuri Baars
 * @author Martijn Hensema
 * @package Assignment7
 */
public class GetRefereeTweetsBolt extends BaseRichBolt {
	private OutputCollector _collector;
	Map<String, String> _map;
	private ArrayList<String> refereesTokenized;
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tweet", "referee_name"));
	}

	@Override
	public void execute(Tuple input) {
		String tweet = ((String) input.getValue(0)).toLowerCase();
	
			for (int i = 0; i < this.refereesTokenized.size(); i++) {
				if (tweet.contains(this.refereesTokenized.get(i))) {
					this._collector.emit(new Values(tweet, this.refereesTokenized.get(i)));
				}
			}
		
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector collector) {
		this._collector = collector;
	//	this._map = map;

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				getClass().getClassLoader().getResourceAsStream(
						"worldcup-games.json")));
		StringBuilder sb = new StringBuilder();
		String line;
		try {
			while ((line = reader.readLine()) != null)
				sb.append(line);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		JSONParser parser = new JSONParser();
		JSONArray collection = null;
		try {
			collection = (JSONArray) parser.parse(sb.toString());
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		this.refereesTokenized = new ArrayList<String>();
		this.refereesTokenized.add("referee");
		this.refereesTokenized.add("referees");
		
		Iterator iter = collection.iterator();
		while (iter.hasNext()) {
			// JSONObject entry =
			try {
				JSONObject game = (JSONObject) iter.next();

				JSONArray officials = (JSONArray) game.get("officials");
				JSONObject home = (JSONObject) game.get("home");
				JSONObject away = (JSONObject) game.get("away");

				String referee_name = "";
				String home_name = (String) home.get("name");
				String away_name = (String) away.get("name");

				SimpleDateFormat formatter = new SimpleDateFormat(
						"dd MMM yyyy - k:mm");
				Date matchTime = new Date();
				matchTime = formatter.parse((String) game.get("time"));
				Iterator i = officials.iterator();

				while (i.hasNext()) {
					JSONObject official = (JSONObject) i.next();
					if (official.get("role").equals("Referee")) {
						referee_name = (String) official.get("name");
					}
				}
				// System.out.println(referee_name);
				Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
			    String nfdNormalizedString = "";	
			    
			 //   for (String token: referee_name.split("\\s+")) {
			    	nfdNormalizedString = Normalizer.normalize(referee_name, Normalizer.Form.NFD); 
			    	String _token = (String)pattern.matcher(nfdNormalizedString.toLowerCase()).replaceAll("");
			    	System.out.println(_token);
			    	if (!this.refereesTokenized.contains(_token)) {
			    		this.refereesTokenized.add(_token);
			    	}
			    //}
			} catch (ClassCastException e) {
				System.out.println("ClassCass");
				e.printStackTrace();
				return; // do nothing (we might log this)
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Prepared with " + new Integer(this.refereesTokenized.size()).toString() + "words");
	}
}
