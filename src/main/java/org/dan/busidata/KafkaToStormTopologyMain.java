package org.dan.busidata;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

public class KafkaToStormTopologyMain {
    private static String zkConnString = "nn1-ha:2181,nn2:2181,nn2-ha:2181";
    //private static String zkConnString = "nn1-ha:2181";
    private static String topicName = "test12345";

    public static void main(String[] args) throws Exception {
        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, "kafkaSpout");
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafkaSpout", kafkaSpout, 1);
        topologyBuilder.setBolt("parserOrderBolt", new ParserOrderMqBolt(), 1).shuffleGrouping("kafkaSpout");
        Config config = new Config();
        config.setNumWorkers(1);
        if(args.length > 0 ){
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("kafka2Storm", config, topologyBuilder.createTopology());
        }
    }
}
