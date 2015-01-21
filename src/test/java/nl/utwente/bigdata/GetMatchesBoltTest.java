package nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

import nl.utwente.bigdata.bolts.GetMatchesBolt;
import nl.utwente.bigdata.bolts.GetRefereeTweetsBolt;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class GetMatchesBoltTest {
	private GetMatchesBolt bolt; 
	private BasicOutputCollector collector;
	private OutputCollector output;
	private Collector col;
	private Config config = new Config();
	private TopologyContext context;
	private static Logger log = LoggerFactory.getLogger(GetMatchesBoltTest.class);

	// prepare bolt and output
	@Before
	public void before() {
		bolt = new GetMatchesBolt();
		col = new Collector();
		output = new OutputCollector(col);
		collector = new BasicOutputCollector(output);
		context = null;
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testMatchMatcher() throws TwitterException {
		
		String tweetJson = "{\"filter_level\": \"medium\", \"retweeted\": false, \"in_reply_to_screen_name\": null, \"possibly_sensitive\": false, \"truncated\": false, \"lang\": \"bs\", \"in_reply_to_status_id_str\": null, \"id\": 481855395687043072, \"in_reply_to_user_id_str\": null, \"in_reply_to_status_id\": null, \"created_at\": \"Wed Jun 25 17:44:13 +0000 2014\", \"favorite_count\": 0, \"place\": null, \"coordinates\": null, \"text\": \"RT @Livescore_News: GOAL! | Bosnia - Iran 3-1. Vrsajevic 83'! #bihirn #WorldCup\", \"contributors\": null, \"retweeted_status\": {\"filter_level\": \"low\", \"contributors\": null, \"text\": \"GOAL! | Bosnia - Iran 3-1. Vrsajevic 83'! #bihirn #WorldCup\", \"geo\": null, \"retweeted\": false, \"in_reply_to_screen_name\": null, \"possibly_sensitive\": false, \"truncated\": false, \"lang\": \"bs\", \"entities\": {\"trends\": [], \"symbols\": [], \"urls\": [], \"hashtags\": [{\"text\": \"bihirn\", \"indices\": [42, 49] }, {\"text\": \"WorldCup\", \"indices\": [50, 59] }], \"user_mentions\": [] }, \"in_reply_to_status_id_str\": null, \"id\": 481854912553570304, \"source\": \"\", \"in_reply_to_user_id_str\": null, \"favorited\": false, \"in_reply_to_status_id\": null, \"retweet_count\": 3, \"created_at\": \"Wed Jun 25 17:42:18 +0000 2014\", \"in_reply_to_user_id\": null, \"favorite_count\": 0, \"id_str\": \"481854912553570304\", \"place\": null, \"user\": {\"location\": \"Netherlands + Belgium\", \"default_profile\": true, \"statuses_count\": 37310, \"profile_background_tile\": false, \"lang\": \"nl\", \"profile_link_color\": \"0084B4\", \"profile_banner_url\": \"https://pbs.twimg.com/profile_banners/458373689/1403712119\", \"id\": 458373689, \"following\": null, \"favourites_count\": 34, \"protected\": false, \"profile_text_color\": \"333333\", \"verified\": false, \"description\": \"Livescores of: Premier League, Primera Division, Serie A, Bundesliga, Ligue 1, Eredivisie, Champions League, Europa League and other competitions / Teams!\", \"contributors_enabled\": false, \"profile_sidebar_border_color\": \"C0DEED\", \"name\": \"Livescore Football\", \"profile_background_color\": \"C0DEED\", \"created_at\": \"Sun Jan 08 13:55:47 +0000 2012\", \"default_profile_image\": false, \"followers_count\": 21935, \"profile_image_url_https\": \"https://pbs.twimg.com/profile_images/463395127604416513/67slRmy6_normal.jpeg\", \"geo_enabled\": false, \"profile_background_image_url\": \"http://abs.twimg.com/images/themes/theme1/bg.png\", \"profile_background_image_url_https\": \"https://abs.twimg.com/images/themes/theme1/bg.png\", \"follow_request_sent\": null, \"url\": \"http://instagram.com/livescorenews\", \"utc_offset\": 7200, \"time_zone\": \"Amsterdam\", \"notifications\": null, \"profile_use_background_image\": true, \"friends_count\": 21, \"profile_sidebar_fill_color\": \"DDEEF6\", \"screen_name\": \"Livescore_News\", \"id_str\": \"458373689\", \"profile_image_url\": \"http://pbs.twimg.com/profile_images/463395127604416513/67slRmy6_normal.jpeg\", \"listed_count\": 94, \"is_translator\": false }, \"coordinates\": null }, \"geo\": null, \"entities\": {\"trends\": [], \"symbols\": [], \"urls\": [], \"hashtags\": [{\"text\": \"bihirn\", \"indices\": [62, 69] }, {\"text\": \"WorldCup\", \"indices\": [70, 79] }], \"user_mentions\": [{\"id\": 458373689, \"name\": \"Livescore Football\", \"indices\": [3, 18], \"screen_name\": \"Livescore_News\", \"id_str\": \"458373689\"}] }, \"source\": \"\", \"favorited\": false, \"in_reply_to_user_id\": null, \"retweet_count\": 0, \"id_str\": \"481855395687043072\", \"user\": {\"location\": \"\", \"default_profile\": false, \"statuses_count\": 76776, \"profile_background_tile\": false, \"lang\": \"en\", \"profile_link_color\": \"D02B55\", \"profile_banner_url\": \"https://pbs.twimg.com/profile_banners/25748883/1372066486\", \"id\": 25748883, \"following\": null, \"favourites_count\": 106, \"protected\": false, \"profile_text_color\": \"3E4415\", \"verified\": false, \"description\": \"Daydreaming.\", \"contributors_enabled\": false, \"profile_sidebar_border_color\": \"829D5E\", \"name\": \"Mr. Man\", \"profile_background_color\": \"352726\", \"created_at\": \"Sat Mar 21 23:49:34 +0000 2009\", \"default_profile_image\": false, \"followers_count\": 548, \"profile_image_url_https\": \"https://pbs.twimg.com/profile_images/474878143288049664/5_8Fa4ZK_normal.jpeg\", \"geo_enabled\": true, \"profile_background_image_url\": \"http://pbs.twimg.com/profile_background_images/257894118/return-of-simba.jpg\", \"profile_background_image_url_https\": \"https://pbs.twimg.com/profile_background_images/257894118/return-of-simba.jpg\", \"follow_request_sent\": null, \"url\": \"http://Instagram.com/stuffedbox\", \"utc_offset\": 3600, \"time_zone\": \"West Central Africa\", \"notifications\": null, \"profile_use_background_image\": true, \"friends_count\": 425, \"profile_sidebar_fill_color\": \"99CC33\", \"screen_name\": \"habeelz\", \"id_str\": \"25748883\", \"profile_image_url\": \"http://pbs.twimg.com/profile_images/474878143288049664/5_8Fa4ZK_normal.jpeg\", \"listed_count\": 8, \"is_translator\": false } }";
		String testCreatedAt = "Wed Jun 25 17:43:17 +0000 2014";
		String[] desiredOutput = {"test", "en", "Wed Jun 25 17:43:17 +0000 2014", "Iran"};
		// the year minus 1900
		//Date expectedMatchTime = new Date(114, 6, 8, 17, 0);
		bolt = new GetMatchesBolt();
		bolt.prepare(config, context, output);
	    bolt.execute(generateTestTuple(TwitterObjectFactory.createStatus(tweetJson), "en", testCreatedAt));

		assertEquals("Iran", col.output.get(0).get(2));
	}
		
	@SuppressWarnings("rawtypes")
	private Tuple generateTestTuple(Object tweet,  String lang, String createdAt) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("tweet", "normalized_tweet");
            }
        };
        return new TupleImpl(topologyContext, new Values(tweet, lang), 1, "");
    }
	
}
