package nl.utwente.bigdata.topology;

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
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public abstract class AbstractTopologyRunner {
	
	// sub-classes have to override this function
	protected abstract StormTopology buildTopology(Properties properties);
	
    public void runLocal(String name, Properties properties) { 
    	StormTopology topology = buildTopology(properties);
        Config conf = new Config();        
        LocalCluster cluster = new LocalCluster();        
        cluster.submitTopology(name, conf, topology);        
        Utils.sleep(Integer.parseInt(properties.getProperty("sleep", 60 * 1000 + "")));
        cluster.shutdown();
    }
    
    // start 
    public void runCluster(String name, Properties properties) throws AlreadyAliveException, InvalidTopologyException {	     
    	StormTopology topology = buildTopology(properties);    	
        Config conf = new Config(); 
        System.out.println(conf.toString());
        StormSubmitter.submitTopology(name, conf, topology);
    }
    
    // Starts a topology based on it's command line 
    public void run(String[] args) {
    	try {
        	String name = args.length >= 1 ? args[0] : "tweet";
        	String type = args.length >= 2 ? args[1] : "local";
        	Properties properties = new Properties();        	
        	if (args.length > 2) {
        		properties.load(new FileInputStream(args[2]));
        	}
        	if (type.equals("local")) {
        		runLocal(name, properties);
        	} else {
        		runCluster(name, properties);
        	}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
    }
    
}
