package nl.utwente.bigdata.bolts;

import java.util.Map;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Bolt which tokenizes all referees from World Cup Data
 * @TODO: implement
 * @TODO: kan vervangen worden door generieke tokenizer
 * @author Djuri Baars
 * @author Martijn Hensema
 * @package Assignment7 
 */
public class TokenizeRefereesBolt extends BaseBasicBolt {
	  private static final long serialVersionUID = 394263766896592119L;
	  private String field;

	  @Override
	  public void prepare(Map stormConf, TopologyContext context) {
		  this.field = "referee_name";
	  }
	  
	  @Override
	  public void execute(Tuple tuple, BasicOutputCollector collector) {
		String val = tuple.getStringByField(this.field);
		for (String token: val.split("\\s+")) {
	    	collector.emit(new Values(token.toLowerCase()));
	    }
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  declarer.declare(new Fields("referee_name"));
	  }

}