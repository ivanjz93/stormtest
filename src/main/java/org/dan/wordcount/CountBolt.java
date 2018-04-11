package org.dan.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseRichBolt {

    private Map<String, Integer> results = new HashMap<String, Integer>();

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    public void execute(Tuple input) {
            String word = input.getString(0);
            if(results.containsKey(word)){
                int count = results.get(word);
                results.put(word, ++count);
            } else {
                results.put(word, 1);
            }
            System.out.println(results);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
