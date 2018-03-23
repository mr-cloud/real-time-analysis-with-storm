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

/*import udacity.storm.IntermediateRankingBolt;
import udacity.storm.TotalRankingBolt;
*/
class TopNTweetTopology
{
  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * In order to create the spout, you need to get twitter credentials
     * If you need to use Twitter firehose/Tweet stream for your idea,
     * create a set of credentials by following the instructions at
     *
     * https://dev.twitter.com/discussions/631
     *
     */

    // now create the tweet spout with the credentials
    TweetSpout tweetSpout = new TweetSpout(
            "[Your customer key]",
            "[Your secret key]",
            "[Your access token]",
            "[Your access secret]"
        );
    // attach the tweet spout to the topology - parallelism of 1
    builder.setSpout(Constant.SPOUT_ID, tweetSpout, 1);
//    builder.setSpout(Constant.SPOUT_ID, new RandomSentenceSpout(), 1);

    // attach the parse tweet bolt using shuffle grouping
    builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping(Constant.SPOUT_ID, Constant.SPOUT_PURE_STREAM_ID);
    // attach the count bolt using fields grouping - parallelism of 15
    //builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));


    /*
    Both rolling-count-bolt and intermediate-rankings-bolt use fieldsGrouping
    to ensure that the same word can be counted cumulatively and sorted with the cumulative counting.
    total-rankings-bolt should keep pace with intermediate-rankings-bolt.
     */

    // attach rolling count bolt using fields grouping - parallelism of 5
    builder.setBolt("rolling-count-bolt", new RollingCountBolt(30, 10), 5).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));
//    builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));


//    builder.setBolt("intermediate-rankings-bolt", new IntermediateRankingsBolt(), 15).fieldsGrouping("count-bolt", new Fields("word", "count"));
    // 因为rolling-count-bolt的分流方式是fieldsGrouping，因此同一个tweet-word只会出现在一个task中，
    // 则输出stream的<"word", "count">与<"word">等价，于是同一个tweet-word也只会出现在同一个intermediate-rankings-bolt task中，
    // intermediate-rankings-bolt中没有排上号的tweet-word是不会出现在total-rankings-bolt的top-N中的。
    // 关键算法在于rolling counter基于滑动窗口的设计，至少保存top-N个槽的计数统计且可能成为top-N的单词被保存（例如top-k space saving算法）。
    builder.setBolt("intermediate-rankings-bolt", new IntermediateRankingsBolt(), 15).fieldsGrouping("rolling-count-bolt", new Fields("word", "count"));

    builder.setBolt("total-rankings-bolt", new TotalRankingsBolt(), 1).globalGrouping("intermediate-rankings-bolt");
    // attach the report bolt using global grouping - parallelism of 1
    //builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("total-rankings-bolt");
    builder.setBolt(Constant.PUSH_TWEET_BOLT_ID, new PushTweetBolt(), 15)
    .shuffleGrouping(Constant.SPOUT_ID, Constant.SPOUT_LOC_STREAM_ID)
    .allGrouping(Constant.TOTAL_RANKINGS_BOLT_ID);
    
    builder.setBolt(Constant.HOT_TWEET_REPORT_BOLT_ID, new HotTweetReportBolt(), 1).globalGrouping(Constant.PUSH_TWEET_BOLT_ID);


    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // let the topology run for 300 seconds. note topologies never terminate!
      Utils.sleep(300000);

      // now kill the topology
      cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
