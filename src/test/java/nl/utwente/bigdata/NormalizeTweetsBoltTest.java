package test.java.nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import nl.utwente.bigdata.bolts.GetRefereeTweetsBolt;
import nl.utwente.bigdata.bolts.NormalizerBolt;
import nl.utwente.bigdata.bolts.TokenizeRefereesBolt;
import nl.utwente.bigdata.bolts.TokenizerBolt;

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
	public void testNormalizer() {
		bolt = new NormalizerBolt();		
        bolt.prepare(config, context);
		String testTweet = "Referee #haïmoudi....what a joke...what a joke...even @FIFAWorldCup @FIFAcom doesn't take 3rd place matches serious #BRANET #WorldCup;";
		String expectedNormalizedString = "referee #haimoudi....what a joke...what a joke...even @fifaworldcup @fifacom doesn't take 3rd place matches serious #branet #worldcup;";
		bolt.execute(generateTestTuple(testTweet), collector);
		assertEquals(new Values(new Values(expectedNormalizedString)), col.output);
	}
	
	@SuppressWarnings("rawtypes")
	private Tuple generateTestTuple(Object message) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("words");
            }
        };
        return new TupleImpl(topologyContext, new Values(message), 1, "");
    }
}
