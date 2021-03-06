package nl.utwente.bigdata;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

public abstract class AbstractTopologyRunnerNimbus {
	
	// sub-classes have to override this function
	protected abstract StormTopology buildTopology(Properties properties);
	
    public void runLocal(String name, Properties properties) { 
    	StormTopology topology = buildTopology(properties);
        Config conf = new Config();        
        LocalCluster cluster = new LocalCluster();        
        cluster.submitTopology(name, conf, topology);        
     //   Utils.sleep(Integer.parseInt(properties.getProperty("sleep", 60 * 1000 + "")));
       // cluster.shutdown();
    }
    
    // start 
    public void runCluster(String name, Properties properties) throws AlreadyAliveException, InvalidTopologyException {	     
    	StormTopology topology = buildTopology(properties);    	
        Config conf = new Config(); 
        //conf.put(Config.NIMBUS_HOST, properties.getProperty("nimbus", "ctit048.ewi.utwente.nl"));
        System.out.println("Here comes the config...");
        System.out.println(conf.toString());
        StormSubmitter.submitTopology(name, conf, topology);
    }
    
    // Starts a topology based on it's command line 
    public void run(String[] args) {
    	try {
        	String name = args[0];
        	String type = args[1];
        	Properties properties = new Properties();        	
        	if (args.length > 2) {
        		properties.load(new FileInputStream(args[2]));
        	}
        	
        	runCluster(name, properties);

		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
}
