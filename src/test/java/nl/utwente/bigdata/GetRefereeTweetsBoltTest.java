package nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class GetRefereeTweetsBoltTest {
	private GetRefereeTweetsBolt bolt; 
	private BasicOutputCollector collector;
	private OutputCollector output;
	private Collector col;
	private Map config;
	private TopologyContext context;
	private static Logger log = LoggerFactory.getLogger(GetRefereeTweetsBoltTest.class);

	// prepare bolt and output
	@Before
	public void before() {
		bolt = new GetRefereeTweetsBolt("en");
		col = new Collector();
		output = new OutputCollector(col);
		collector = new BasicOutputCollector(output);
		context = null;
	}
	
	@Test
	public void testContainingTweet() {
		//ArrayList<String> referees = new ArrayList<String>();
		//String[] tokenizedReferees = {"djamel", "haimoudi", "cakir"};
		String tweet = "referee #haimoudi....what a joke...what a joke...even @fifaworldcup @fifacom doesn't take 3rd place matches serious #branet #worldcup;";
		
		bolt = new GetRefereeTweetsBolt("en");
        bolt.prepare(config, context, output);
        bolt.execute(generateTestTuple(tweet));
        assertEquals(1, col.output.size());
	}
//	
//	@Test
//	public void testNonContainingTweet() {
//		String[] tokenizedReferees = {"djamel", "haimoudi", "cakir"};
//		String tweet = "Random #geblaat";
//		bolt = new GetRefereeTweetsBolt();
//		
//        bolt.prepare(config, context);
//        bolt.execute(generateTestTuple(tweet, Arrays.asList(tokenizedReferees)), collector);
//        assertEquals(0, col.output.size());
//	}
		
	@SuppressWarnings("rawtypes")
	private Tuple generateTestTuple(String tweet) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("tweet", "normalized_text", "lang");
            }
        };
        return new TupleImpl(topologyContext, new Values(tweet, tweet, "en"), 1, "");
    }
	
}
