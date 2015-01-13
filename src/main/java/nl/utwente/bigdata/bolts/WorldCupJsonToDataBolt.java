/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.utwente.bigdata.bolts;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Extracts world cup data from World Cup JSON 
 * and emits the referee name, home team, out team and date of the match
 * 
 * @TODO: implement
 * 
 * @author Djuri Baars
 * @author Martijn Hensema
 * @package Assignment7 
 */
public class WorldCupJsonToDataBolt extends BaseBasicBolt {
  private transient JSONParser parser;
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
	  parser = new JSONParser();
  }
  
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
	  try {
		  JSONObject game = (JSONObject) parser.parse(tuple.getString(0));
        
		  JSONArray officials = (JSONArray) game.get("officials");
        JSONObject home = (JSONObject) game.get("home");
        JSONObject away = (JSONObject) game.get("away");
        
        String referee_name = "";
        String home_name = (String) home.get("name");
        String away_name = (String) away.get("name");
        
        SimpleDateFormat formatter = new SimpleDateFormat("dd MMM yyyy - k:mm");
        Date matchTime = new Date();
        matchTime = formatter.parse((String) game.get("time"));
        Iterator i = officials.iterator();
        
        while (i.hasNext()) {
            JSONObject official = (JSONObject) i.next();
            if (official.get("role").equals("Referee")) {
            	referee_name = (String) official.get("name");
            }
        }
        //System.out.println(referee_name);
        collector.emit(new Values(referee_name, home_name, away_name, matchTime));
      }
      catch (ClassCastException e) {  
    	System.out.println("ClassCass");
    	e.printStackTrace();
        return; // do nothing (we might log this)
      }
      catch (org.json.simple.parser.ParseException e) {
    	System.out.println("ParseException");
    	e.printStackTrace();
        return; // do nothing 
      } catch (Exception e) {
    	  e.printStackTrace();
      }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  declarer.declare(new Fields("referee_name", "home", "out", "time"));
  }

}
