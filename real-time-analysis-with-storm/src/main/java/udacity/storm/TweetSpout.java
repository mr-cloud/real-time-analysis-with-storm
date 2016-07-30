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

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.StallWarning;
import twitter4j.GeoLocation;
import twitter4j.FilterQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Date;
import java.util.Random;

import udacity.storm.tools.*;

/**
 * A spout that uses Twitter streaming API for continuously
 * getting tweets
 */
public class TweetSpout extends BaseRichSpout 
{
	  //get county id from geolocation
	  CountiesLookup clookup ;

  // Twitter API authentication credentials
  String custkey, custsecret;              
  String accesstoken, accesssecret;

  // To output tuples from spout to the next stage bolt
  SpoutOutputCollector collector;

  // Twitter4j - twitter stream to get tweets
  TwitterStream twitterStream;

  // Shared queue for getting buffering tweets received
  LinkedBlockingQueue<String> queue = null;
  
  // Class for listening on the tweet stream - for twitter4j
  private class TweetListener implements StatusListener {
	  Random _rand = new Random();
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
      long tweetWithGeo = 0;
      long tweetWithoutGeo = 0;
    // Implement the callback function when a tweet arrives
    @Override
    public void onStatus(Status status) 
    {

        String geoInfo = null;
        boolean geoTag = false;
      // add the tweet into the queue buffer
      //queue.offer(status.getText() + ", created at: " + status.getCreatedAt().toString());
    	
    	if(status.getGeoLocation() != null){
    			GeoLocation geo = status.getGeoLocation();
    			geoInfo = String.valueOf(geo.getLatitude())
    					+ ","
    					+ String.valueOf(geo.getLongitude());
    			tweetWithGeo++;
    			geoTag = true;
    		}
    	else{
    	    geoInfo = geoInfos[_rand.nextInt(geoInfos.length)];
			tweetWithoutGeo++;
    	}
    	System.out.println("-----------------tweets with geolocation ratio: " + 100.0*tweetWithGeo/(tweetWithGeo + tweetWithoutGeo) + "%-----------------");
		queue.offer(
				(geoTag?"":"(FalseGeoInfo)")
				+ status.getText() 
				+ ". created at: " + status.getCreatedAt().toString()
				+ Constant.GEOLOCATION_DELIMITER
				+ geoInfo);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn) 
    {
    }

    @Override
    public void onTrackLimitationNotice(int i) 
    {
    }

    @Override
    public void onScrubGeo(long l, long l1) 
    {
    }

    @Override
    public void onStallWarning(StallWarning warning) 
    {
    }

    @Override
    public void onException(Exception e) 
    {
      e.printStackTrace();
    }
  };

  /**
   * Constructor for tweet spout that accepts the credentials
   */
  public TweetSpout(
      String                key, 
      String                secret, 
      String                token, 
      String                tokensecret) 
  {
    custkey = key;
    custsecret = secret;
    accesstoken = token;
    accesssecret = tokensecret;
  }
  
  @Override
  public void open(
      Map                     map,
      TopologyContext         topologyContext,
      SpoutOutputCollector    spoutOutputCollector)
  {
	    clookup= new CountiesLookup();

    // create the buffer to block tweets
    queue = new LinkedBlockingQueue<String>(1000);

    // save the output collector for emitting tuples
    collector = spoutOutputCollector;


    // build the config with credentials for twitter 4j
    ConfigurationBuilder config = 
            new ConfigurationBuilder()
                   .setOAuthConsumerKey(custkey)
                   .setOAuthConsumerSecret(custsecret)
                   .setOAuthAccessToken(accesstoken)
                   .setOAuthAccessTokenSecret(accesssecret)
                   //set for proxy
                   /*                   .setHttpProxyHost("127.0.0.1")
                   .setHttpProxyPort(8118)*/
                   ;

    // create the twitter stream factory with the config
    TwitterStreamFactory fact = 
        new TwitterStreamFactory(config.build());

    // get an instance of twitter stream
    twitterStream = fact.getInstance();

    //filter the tweets generated in USA
    FilterQuery tweetFilterQuery = new FilterQuery(); // See 
    //tweets with geo info are rather rare. do not filter with the one below.
/*    tweetFilterQuery.locations(new double[][]{new double[]{-124.848974,24.396308},
                    new double[]{-66.885444,49.384358
                    }}); */
    
    tweetFilterQuery.language(new String[]{"en"});

    // provide the handler for twitter stream
    twitterStream.addListener(new TweetListener());
    twitterStream.filter(tweetFilterQuery);

    // start the sampling of tweets
    twitterStream.sample();
  }

  @Override
  public void nextTuple() 
  {
    // try to pick a tweet from the buffer
    String ret = queue.poll();

    // if no tweet is available, wait for 50 ms and return
    if (ret==null) 
    {
      Utils.sleep(50);
      return;
    }

    // now emit the tweet to next stage bolt
   // collector.emit(new Values(ret));
    String tweet = ret.split(Constant.GEOLOCATION_DELIMITER)[0];
    String geoInfo = ret.split(Constant.GEOLOCATION_DELIMITER)[1];
    double latitude = Double.parseDouble(geoInfo.split(",")[0]);
    double longitude = Double.parseDouble(geoInfo.split(",")[1]);
    String county_id = clookup.getCountyCodeByGeo(latitude, longitude);

    collector.emit(Constant.SPOUT_PURE_STREAM_ID, new Values(tweet));
    
    collector.emit(Constant.SPOUT_LOC_STREAM_ID, new Values(county_id + Constant.GEOLOCATION_DELIMITER + tweet));
  }

  @Override
  public void close() 
  {
    // shutdown the stream - when we are going to exit
    twitterStream.shutdown();
  }

  /**
   * Component specific configuration
   */
  @Override
  public Map<String, Object> getComponentConfiguration() 
  {
    // create the component config 
    Config ret = new Config();
 
    // set the parallelism for this spout to be 1
    ret.setMaxTaskParallelism(1);

    return ret;
  }    

  @Override
  public void declareOutputFields(
      OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet'
    //outputFieldsDeclarer.declare(new Fields("tweet"));
	  outputFieldsDeclarer.declareStream(Constant.SPOUT_PURE_STREAM_ID, new Fields("tweet"));  
	  outputFieldsDeclarer.declareStream(Constant.SPOUT_LOC_STREAM_ID, new Fields("tweet-with-geo"));

  }
}
