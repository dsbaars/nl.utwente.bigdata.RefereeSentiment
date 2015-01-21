package nl.utwente.bigdata.bolts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Pattern;

import nl.utwente.bigdata.RefereeSentiment;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import twitter4j.Status;
import backtype.storm.Config;
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
	public static final Logger logger = Logger.getLogger(GetRefereeTweetsBolt.class);  

	Map<String, String> _map;
	private ArrayList<String> refereesTokenized;
	private String language;
	
	public GetRefereeTweetsBolt(Config conf) {
		this.language = (String) conf.get("language");
	}

	public GetRefereeTweetsBolt(String string) {
		this.language = string;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tweet", "normalized_text", "referee_name"));
	}

	@Override
	public void execute(Tuple input) {
		String tweet = input.getStringByField("tweet");

		String normalized_text = ((String) input.getStringByField("normalized_text")).toLowerCase();
		//logger.info(tweet);
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
		this._map = map;

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				getClass().getClassLoader().getResourceAsStream(
						"wcdata/worldcup-games.json")));
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
		
		final String refereeFilePath = "lang/" + this.language + "/refereewords.txt";
		this.refereesTokenized = new ArrayList<String>();

		try {
			final URL url = getClass().getClassLoader().getResource(
					refereeFilePath);
			final String text = Resources.toString(url, Charsets.UTF_8);
			final Iterable<String> lineSplit = Splitter.on("\n").trimResults().omitEmptyStrings().split(text);
			List<String> tabSplit;
			for (final String str: lineSplit) {
				this.refereesTokenized.add(str);
			}
		} catch (final IOException e) {
			e.printStackTrace();
			//Should not occur. If it occurs, we cant continue. So, exiting at this point itself.
			System.exit(1);
		}

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
			    	//System.out.println(_token);
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
