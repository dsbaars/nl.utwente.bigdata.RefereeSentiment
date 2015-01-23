package nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import nl.utwente.bigdata.bolts.NormalizerBolt;
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

public class NormalizeTweetsBoltTest {
	private NormalizerBolt bolt; 
	private BasicOutputCollector collector;
	private OutputCollector output;
	private Collector col;
	private Config config = new Config();
	private TopologyContext context;
	private static Logger log = LoggerFactory.getLogger(NormalizeTweetsBoltTest.class);

	// prepare bolt and output
	@Before
	public void before() {
		bolt = new NormalizerBolt();
		col = new Collector();
		collector = new BasicOutputCollector(new OutputCollector(col));
		context = null;
	}
	
	@Test
	public void testNormalizer() throws TwitterException {
		bolt = new NormalizerBolt();		
        bolt.prepare(config, context);
		String testTweet = "{ \"text\": \"Referee #haïmoudi....what a joke...what a joke...even @FIFAWorldCup @FIFAcom doesn't take 3rd place matches serious #BRANET #WorldCup;\", \"lang\": \"nl\" }";
		String expectedNormalizedString = "referee #haimoudi....what a joke...what a joke...even @fifaworldcup @fifacom doesn't take 3rd place matches serious #branet #worldcup;";
		bolt.execute(generateTestTuple(TwitterObjectFactory.createStatus(testTweet)), collector);
	//	assertEquals(new Values(new Values(expectedNormalizedString, "nl")), col.output);
		assertEquals(col.output.get(0).get(1), expectedNormalizedString);
		assertEquals(col.output.get(0).get(2), "nl");
	}
	
	@SuppressWarnings("rawtypes")
	private Tuple generateTestTuple(Object message) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("tweet");
            }
        };
        return new TupleImpl(topologyContext, new Values(message), 1, "");
    }
}
