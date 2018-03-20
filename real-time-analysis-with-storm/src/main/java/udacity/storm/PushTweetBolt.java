package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

import udacity.storm.tools.*;
import java.util.List;

/**
 * A bolt that counts the words that it receives
 */
public class PushTweetBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  // Map to store the top hash tags.
  private Map<String, Long> tagMap;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;

    // create and initialize the map
    tagMap = new HashMap<String, Long>();
  }

  @Override
  public void execute(Tuple tuple)
  {
	  String componentId = tuple.getSourceComponent();
	  //tuple comes from the spout 
	  if(componentId.equals(Constant.SPOUT_ID)){
		  if(tagMap.size() == 0){
			  //collector.emit(new Values("DELIMETER" + "wait a moment!" + "DELIMETER", new Long(3)));
		  }
		  else{
			  String tweet = tuple.getString(0);
			    // provide the delimiters for splitting the tweet
			    String delims = "[ .,?!]+";

			    // now split the tweet into tokens
			    String[] tokens = tweet.split(delims);

			    for (String token: tokens) {
			    	if(token.startsWith("#")){
			    		if(tagMap.containsKey(token)){
			    			collector.emit(new Values(tweet, tagMap.get(token)));
			    			break;
			    		}
			    	}
			      }
		  }
	  }
	  else if(componentId.equals(Constant.TOTAL_RANKINGS_BOLT_ID)){
		  tagMap.clear();
		  Rankings mergedRankings = (Rankings) tuple.getValue(0);
		  List<Rankable> rankables = mergedRankings.getRankings();
		  String tag = new String();
		  long count = 0;
		  for(Rankable r: rankables){
			  tag = (String)r.getObject();
			  count = r.getCount();
			  tagMap.put(tag, count);
		  }
	  }
	  else{
		  
	  }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a two columns called 'word' and 'count'

    // declare the first column 'tweet', second column 'hot'
	  // Filter out tweets with hot-indication carrying recent hot tag.
    outputFieldsDeclarer.declare(new Fields(Constant.PUSH_TWEET_BOLT_FIELD_0, Constant.PUSH_TWEET_BOLT_FIELD_1));
  }
}
