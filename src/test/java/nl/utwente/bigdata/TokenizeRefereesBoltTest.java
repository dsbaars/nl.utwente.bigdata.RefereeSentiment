package nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.HashMap;
import nl.utwente.bigdata.bolts.TokenizeRefereesBolt;

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

public class TokenizeRefereesBoltTest {
	private TokenizeRefereesBolt bolt; 
	private BasicOutputCollector collector;
	private OutputCollector output;
	private Collector col;
	private Config config = new Config();
	private TopologyContext context;
	private static Logger log = LoggerFactory.getLogger(GetRefereeTweetsBoltTest.class);

	// prepare bolt and output
	@Before
	public void before() {
		bolt = new TokenizeRefereesBolt();
		col = new Collector();
		collector = new BasicOutputCollector(new OutputCollector(col));
		context = null;
	}
	
	@Test
	public void testRetrieval() {
		bolt = new TokenizeRefereesBolt();		
        bolt.prepare(config, context);
		bolt.execute(generateTestTuple("Marco RODRIGUEZ", "Brazil", "Germany", new Date()), collector);
		bolt.execute(generateTestTuple("Cuneyt CAKIR", "Brazil", "Germany", new Date()), collector);
		assertEquals(4, col.output.size());
	}
	
	@SuppressWarnings("rawtypes")
	private Tuple generateTestTuple(String message, String home, String out, Date time) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("referee_name", "home", "out", "time");
            }
        };
        return new TupleImpl(topologyContext, new Values(message, home, out, time), 1, "");
    }
	

	
}
