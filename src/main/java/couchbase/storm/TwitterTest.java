package couchbase.storm;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import java.util.Map;
import storm.starter.spout.TwitterSampleSpout;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class TwitterTest {

    public static final String USERNAME = "YOUR_USERNAME";
    public static final String PASSWORD = "YOUR_PASSWORD";
    
    
    public static class CouchbaseBolt extends ShellBolt implements IRichBolt {
        
        public CouchbaseBolt() {
            super("python", "couchbaseconnector.py");
        }
        
        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
        }

    }
    
    
    public static void main(String[] args) throws Exception {
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("spout", new TwitterSampleSpout(USERNAME, PASSWORD), 5);
        builder.setBolt("save", new CouchbaseBolt()).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);

        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {        
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("couchbase-bolt", conf, builder.createTopology());
        }
    }
}
