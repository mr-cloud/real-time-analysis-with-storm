package udacity.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import udacity.storm.tools.*;

public class RandomSentenceSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;

  //get county id from geolocation
  CountiesLookup clookup ;


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
    clookup= new CountiesLookup();

  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    String[] sentences = new String[]{
      "hello world",
      "hello #storm",
      "hello #java",
      "hello #python",
      "hello #MLGB",
      "hello world",
      "hello #storm",
      "hello #java",
      "hello #python",
      "hello #MLGB",
      "hello world",
      "hello #storm",
      "hello #java",
      "hello #python",
      "hello #MLGB",
      "hello world",
      "hello #storm",
      "hello #java",
      "hello #python",
      "hello #MLGB",
      "hello world",
      "hello #storm",
      "hello #java",
      "hello #python",
      "hello #MLGB",
      "hello #Akilis"
      };
    //randomized geolocation
    String[] geoInfos = new String[]{
    		"32.532160,-86.646469",
    		"30.659218,-87.746067",
    		"38.781102,-90.674915",
    		"44.652419,-71.289383",
    		"36.509669,-106.693983",
    		"35.920951,-81.177467",
    		"46.096815,-102.533198",
    		"33.964004,-96.264137",
    		"18.436163,-66.336673",
    		"17.971485,-66.262252"
    };
    String sentence = sentences[_rand.nextInt(sentences.length)];
//    _collector.emit(new Values(sentence));
    String geoInfo = geoInfos[_rand.nextInt(geoInfos.length)];
    double latitude = Double.parseDouble(geoInfo.split(",")[0]);
    double longitude = Double.parseDouble(geoInfo.split(",")[1]);
    String county_id = clookup.getCountyCodeByGeo(latitude, longitude);

    _collector.emit(Constant.SPOUT_PURE_STREAM_ID, new Values(sentence));
    
    _collector.emit(Constant.SPOUT_LOC_STREAM_ID, new Values(county_id + Constant.GEOLOCATION_DELIMITER + sentence));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
//    declarer.declare(new Fields("sentence"));
//	    declarer.declare(Constant.SPOUT_PURE_STREAM_ID, new Fields("sentence"));
	    declarer.declareStream(Constant.SPOUT_PURE_STREAM_ID, new Fields("sentence"));  
	  declarer.declareStream(Constant.SPOUT_LOC_STREAM_ID, new Fields("sentence-with-geo"));
  }

}
