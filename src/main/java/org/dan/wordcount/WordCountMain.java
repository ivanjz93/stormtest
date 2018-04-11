package org.dan.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountMain {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("mySpout", new MySpout(), 2);
        builder.setBolt("myBolt1", new SplitBolt(), 2).shuffleGrouping("mySpout");
        builder.setBolt("myBolt2", new CountBolt(), 4).fieldsGrouping("myBolt1", new Fields("word"));

        Config config = new Config();
        config.setNumWorkers(3);

        StormSubmitter.submitTopology(args[0], config, builder.createTopology());

//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("wordcount ", config, builder.createTopology());

    }
}
