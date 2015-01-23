package nl.utwente.bigdata.topology;

import java.util.Properties;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

public abstract class AbstractTopologyRunner {
	public static final Logger logger = Logger.getLogger(AbstractTopologyRunner.class);  
	
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
    
    // start 
    public void runCluster(String name, Properties properties) throws AlreadyAliveException, InvalidTopologyException {	     
    	StormTopology topology = buildTopology(properties);    	
        Config conf = new Config(); 
        conf.put(Config.NIMBUS_HOST, properties.getProperty("nimbus", "ctit048"));	
        System.out.println(conf.toString());
        StormSubmitter.submitTopology(name, conf, topology);
    }
    
    // Starts a topology based on it's command line 
    public void run(String[] args) {
    	String name = args[0];
//        	String type = args[1];
		Properties properties = new Properties();        	
		runLocal(name, properties);
    }
    
}
