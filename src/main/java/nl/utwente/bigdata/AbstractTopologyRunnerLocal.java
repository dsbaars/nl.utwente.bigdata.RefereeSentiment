package nl.utwente.bigdata;

import java.util.Properties;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

public abstract class AbstractTopologyRunnerLocal {
	public static final Logger logger = Logger.getLogger(AbstractTopologyRunnerLocal.class);  
	
	// sub-classes have to override this function
	protected abstract StormTopology buildTopology(Properties properties);
	
    public void runLocal(String name, Properties properties) { 
    	StormTopology topology = buildTopology(properties);
        Config conf = new Config();
        for (Object key: properties.keySet()) {
        	conf.put((String)key, properties.getProperty((String) key));
        	logger.info("Config set: " + (String)key + " to value: " + properties.getProperty((String) key));
        }
        LocalCluster cluster = new LocalCluster();        
        cluster.submitTopology(name, conf, topology);        
//        Utils.sleep(Integer.parseInt(properties.getProperty("sleep", 60 * 1000 + "")));
//        cluster.shutdown();
    }
        
    // Starts a topology based on it's command line 
    public void run(String[] args) {
    	String name = args[0];
//        	String type = args[1];
		Properties properties = new Properties();        	
		runLocal(name, properties);
    }
    
}
