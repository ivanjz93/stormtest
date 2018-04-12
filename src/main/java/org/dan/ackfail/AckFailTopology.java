package org.dan.ackfail;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class AckFailTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("mySpout", new MySpout(), 2);
        builder.setBolt("myBolt", new MyBolt(), 4).shuffleGrouping("mySpout");

        Config config = new Config();
        config.setNumWorkers(3);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(AckFailTopology.class.getSimpleName(), config, builder.createTopology());

       // StormSubmitter.submitTopology(args[0], config, builder.createTopology());
    }
}
