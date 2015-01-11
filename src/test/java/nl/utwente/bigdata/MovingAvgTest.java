package test.java.nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import nl.utwente.bigdata.bolts.MovingAvgBolt;

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

public class MovingAvgTest {
	MovingAvgBolt bolt; 
	private BasicOutputCollector collector;
	private OutputCollector output;
	private Collector col;
	private Config config = new Config();
	private  TopologyContext context;

	// prepare bolt and output
	@Before
	public void before() {
		bolt = new MovingAvgBolt();
		col = new Collector();
		collector = new BasicOutputCollector(new OutputCollector(col));
		context = null;
	}
	
	@Test
	public void testOneValue() {
		MovingAvgBolt bolt = new MovingAvgBolt();
        bolt.prepare(config, context);
		bolt.execute(generateTestTuple(1.0), collector);
		// the bolt shouldn't emit a value yet
		assertEquals(0, col.output.size());		
	}
	
	@Test
	public void testFiveOnes() {
		MovingAvgBolt bolt = new MovingAvgBolt();
        bolt.prepare(config, context);
		bolt.execute(generateTestTuple(1.0), collector);
		bolt.execute(generateTestTuple(1.0), collector);
		bolt.execute(generateTestTuple(1.0), collector);
		bolt.execute(generateTestTuple(1.0), collector);
		bolt.execute(generateTestTuple(1.0), collector);
		assertEquals(1, col.output.size());
		double[] desiredOutput = new double[] { 1.0 };
		for (int i =  0; i<desiredOutput.length; i++) {
			assertEquals(desiredOutput[i], col.output.get(i).get(0));
		}		
	}
	
	@Test
	public void testSixValues() {
		MovingAvgBolt bolt = new MovingAvgBolt();
        bolt.prepare(config, context);
		bolt.execute(generateTestTuple(1.0), collector);
		bolt.execute(generateTestTuple(2.0), collector);
		bolt.execute(generateTestTuple(3.0), collector);
		bolt.execute(generateTestTuple(4.0), collector);
		bolt.execute(generateTestTuple(5.0), collector);
		bolt.execute(generateTestTuple(6.0), collector);
		
		double[] desiredOutput = new double[] { (1.0 + 2.0 + 3.0 + 4.0 + 5.0) / 5.0, (2.0 + 3.0 + 4.0 + 5.0 + 6.0) / 5.0 };
		assertEquals(desiredOutput.length, col.output.size());
		for (int i =  0; i<desiredOutput.length; i++) {
			assertEquals(desiredOutput[i], col.output.get(i).get(0));
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
