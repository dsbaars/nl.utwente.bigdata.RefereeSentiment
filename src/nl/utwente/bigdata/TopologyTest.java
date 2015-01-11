package nl.utwente.bigdata;

import java.util.Properties;

import nl.utwente.bigdata.AbstractTopologyRunner;
import nl.utwente.bigdata.Assignment4_1;
import nl.utwente.bigdata.Assignment4_2;
import nl.utwente.bigdata.Assignment4_3;

import org.junit.Test;

public class TopologyTest {

//	@Test
//	public void test() throws IOException {
//		Properties properties = new Properties();
//        properties.load(getClass().getResourceAsStream("twitterrc"));
//        properties.put("sleep", String.valueOf(60 * 1000));
//        PrintTwitterTopology topology = new PrintTwitterTopology();
//		topology.runLocal("test", properties);
//	}
//	
//	@Test
//	public void testRandomTweet() {
//		Properties properties = new Properties();
//		SaveRandomTweetTopology topology = new SaveRandomTweetTopology();
//		topology.runLocal("test", properties);
//	}
//
//	
//	@Test
//	public void testRandomTokenziedTweet() {
//		Properties properties = new Properties();
//		PrintRandomTweetTokensTopology topology = new PrintRandomTweetTokensTopology();
//		topology.runLocal("test", properties);
//	}
	
	@Test
	public void testAssignment4_1() {
		Properties properties = new Properties();
		AbstractTopologyRunner runner= new Assignment4_1();
		runner.runLocal("test", properties);
	}
	
	@Test
	public void testAssignment4_2() {
		Properties properties = new Properties();
		properties.put("M", "3");
		properties.put("N", "100");
		AbstractTopologyRunner runner= new Assignment4_2();
		runner.runLocal("test", properties);
	}
	
	@Test
	public void testAssignment4_3() {
		Properties properties = new Properties();
		AbstractTopologyRunner runner= new Assignment4_3();
		runner.runLocal("test", properties);
	}
}
