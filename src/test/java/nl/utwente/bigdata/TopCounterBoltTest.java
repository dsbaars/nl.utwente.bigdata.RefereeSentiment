package test.java.nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import nl.utwente.bigdata.bolts.NGramerBolt;
import nl.utwente.bigdata.bolts.TopCounterBolt;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;

public class TopCounterBoltTest {
	private TopCounterBolt bolt;
	private BasicOutputCollector collector;
	private OutputCollector output;
	private Collector col;
	private Config config = new Config();
	private  TopologyContext context;

	// prepare bolt and output
	@Before
	public void before() {
		bolt = new TopCounterBolt();
		col = new Collector();
		collector = new BasicOutputCollector(new OutputCollector(col));
		context = null;
	}
	
	@Test
	public void test1() throws InterruptedException {		
        bolt.prepare(config, context);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test2"), collector);
		bolt.execute(tuple("test2"), collector);
		bolt.execute(tuple("test3"), collector);
		bolt.execute(tickl(), collector);
		
		String[] desiredOutput = {"hello", "test", "test2"};
		assertEquals(desiredOutput.length, col.output.size());
		for (int i =  0; i<desiredOutput.length; i++) {
			assertEquals(desiredOutput[i], col.output.get(i).get(0));
		}		
	}
	
	@Test
	public void test2() throws InterruptedException {
		config.put("N", "4");
        bolt.prepare(config, context);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test2"), collector);
		bolt.execute(tuple("test2"), collector);
		bolt.execute(tuple("test3"), collector);
		bolt.execute(tickl(), collector);
		
		String[] desiredOutput = {"hello", "test", "test2", "test3"};
		assertEquals(desiredOutput.length, col.output.size());
		for (int i =  0; i<desiredOutput.length; i++) {
			assertEquals(desiredOutput[i], col.output.get(i).get(0));
		}		
	}
	
	@Test
	public void testN_Equals2() throws InterruptedException {
		config.put("N", "2");
        bolt.prepare(config, context);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test2"), collector);
		bolt.execute(tuple("test2"), collector);
		bolt.execute(tuple("test3"), collector);
		bolt.execute(tickl(), collector);
		
		String[] desiredOutput = {"hello", "test"};
		assertEquals(desiredOutput.length, col.output.size());
		for (int i =  0; i<desiredOutput.length; i++) {
			assertEquals(desiredOutput[i], col.output.get(i).get(0));
		}		
	}
	
	@Test
	public void testTwoTickl() throws InterruptedException {
		config.put("N", "2");
        bolt.prepare(config, context);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test"), collector);
		bolt.execute(tuple("test2"), collector);
		bolt.execute(tuple("test2"), collector);
		bolt.execute(tuple("test3"), collector);
		bolt.execute(tickl(), collector);
		bolt.execute(tuple("hello"), collector);
		bolt.execute(tuple("you"), collector);
		bolt.execute(tickl(), collector);
		
		String[] desiredOutput = {"hello", "test", "hello", "you"};
		assertEquals(desiredOutput.length, col.output.size());
		for (int i =  0; i<desiredOutput.length; i++) {
			assertEquals(desiredOutput[i], col.output.get(i).get(0));
		}		
	}
	
	@SuppressWarnings("rawtypes")
	private Tuple tuple(Object message) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("message");
            }
            
            @Override
            public String getComponentId(int x) {
            	return "test";
            }
        };
        return new TupleImpl(topologyContext, new Values(message), 1, "");
    }
	
	private Tuple tickl() {
		TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields();
            }
            
            @Override
            public String getComponentId(int taskId) {
            	return Constants.SYSTEM_COMPONENT_ID;
            }
            
            
        };
        return new TupleImpl(topologyContext, new Values(), 1, Constants.SYSTEM_TICK_STREAM_ID);
	}
	
}
