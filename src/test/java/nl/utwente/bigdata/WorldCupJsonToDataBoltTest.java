package nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import nl.utwente.bigdata.bolts.WorldCupJsonToDataBolt;

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

public class WorldCupJsonToDataBoltTest {
	private WorldCupJsonToDataBolt bolt; 
	private BasicOutputCollector collector;
	private OutputCollector output;
	private Collector col;
	private Config config = new Config();
	private TopologyContext context;
	private static Logger log = LoggerFactory.getLogger(WorldCupJsonToDataBoltTest.class);

	// prepare bolt and output
	@Before
	public void before() {
				
		bolt = new WorldCupJsonToDataBolt();
		col = new Collector();
		collector = new BasicOutputCollector(new OutputCollector(col));
		context = null;
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testParseFormat() {
		String testJson = "{\"officials\":[{\"role\":\"Referee\",\"name\":\"Marco RODRIGUEZ\"},{\"role\":\"Assistant Referee 1\",\"name\":\"Marvin torrentera\"},{\"role\":\"Assistant Referee 2\",\"name\":\"Marcos QUINTERO\"},{\"role\":\"Fourth official\",\"name\":\"Mark GEIGER\"}],\"away\":{\"id\":\"43948\",\"goals\":[],\"name\":\"Germany\"},\"score\":[1,7],\"stadium\":\"Estadio Mineirao\",\"time\":\"08 Jul 2014 - 17:00\",\"home\":{\"id\":\"43924\",\"goals\":[{\"player_id\":\"312868\",\"player\":\"OSCAR\",\"minute\":\"90\"}],\"name\":\"Brazil\"},\"id\":\"300186474\"}";
		String[] desiredOutput = {"Marco RODRIGUEZ", "Brazil", "Germany"};
		// the year minus 1900
		Date expectedMatchTime = new Date(114, 6, 8, 17, 0);
		bolt = new WorldCupJsonToDataBolt();
		
		
        bolt.prepare(config, context);
		bolt.execute(generateTestTuple(testJson), collector);
		List<Object> list = col.output.get(0);
		assertEquals(4, list.size());
		assertEquals(expectedMatchTime, list.get(3));
		for (int i =  0; i< desiredOutput.length; i++) {
			assertEquals(desiredOutput[i], list.get(i));
		}	
	}
		
	@SuppressWarnings("rawtypes")
	private Tuple generateTestTuple(Object message) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("message");
            }
        };
        return new TupleImpl(topologyContext, new Values(message), 1, "");
    }
	
}
