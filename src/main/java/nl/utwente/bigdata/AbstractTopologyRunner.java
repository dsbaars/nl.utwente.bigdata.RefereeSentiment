package nl.utwente.bigdata;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import nl.utwente.bigdata.bolts.PrinterBolt;
import nl.utwente.bigdata.spouts.TwitterSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public abstract class AbstractTopologyRunner {
	
	// sub-classes have to override this function
	protected abstract StormTopology buildTopology(Properties properties);
	
    public void runLocal(String name, Properties properties) { 
    	StormTopology topology = buildTopology(properties);
        Config conf = new Config();
        for (Object key: properties.keySet()) {
        	conf.put((String)key, properties.getProperty((String) key));
        }
        LocalCluster cluster = new LocalCluster();        
        cluster.submitTopology(name, conf, topology);        
        Utils.sleep(Integer.parseInt(properties.getProperty("sleep", 60 * 1000 + "")));
        cluster.shutdown();
    }
    
    // start 
    public void runCluster(String name, Properties properties) throws AlreadyAliveException, InvalidTopologyException {	     
    	StormTopology topology = buildTopology(properties);    	
        Config conf = new Config(); 
        conf.put(Config.NIMBUS_HOST, properties.getProperty("nimbus", "localhost"));	
        System.out.println(conf.toString());
        StormSubmitter.submitTopology(name, conf, topology);
    }
    
    // Starts a topology based on it's command line 
    public void run(String[] args) {
    	try {
        	String name = args[0];
        	String type = args[1];
        	Properties properties = new Properties();        	

        	if (type.equals("local")) {
        		runLocal(name, properties);
        	} else {
        		runCluster(name, properties);
        	}

		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
    }
    
}
