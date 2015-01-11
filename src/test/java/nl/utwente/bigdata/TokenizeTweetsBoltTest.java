package test.java.nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import nl.utwente.bigdata.bolts.GetRefereeTweetsBolt;
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

public class TokenizeTweetsBoltTest {
	private TokenizerBolt bolt; 
	private BasicOutputCollector collector;
	private OutputCollector output;
	private Collector col;
	private Config config = new Config();
	private TopologyContext context;
	private static Logger log = LoggerFactory.getLogger(GetRefereeTweetsBoltTest.class);

	// prepare bolt and output
	@Before
	public void before() {
		bolt = new TokenizerBolt();
		col = new Collector();
		collector = new BasicOutputCollector(new OutputCollector(col));
		context = null;
	}
	
	@Test
	public void testTokenizer() {
		bolt = new TokenizerBolt();		
        bolt.prepare(config, context);
		String testTweet = "Referee #haïmoudi.";
		bolt.execute(generateTestTuple(testTweet), collector);
		
		List<Values> expectedOutput = new ArrayList<Values>();
		expectedOutput.add(new Values("referee"));
		expectedOutput.add(new Values("#haimoudi."));

		
		assertEquals(expectedOutput, col.output);
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
