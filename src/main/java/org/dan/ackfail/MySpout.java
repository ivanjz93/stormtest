package org.dan.ackfail;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class MySpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String[] sentences = {
            "Spring Integration provides an extension of the Spring programming model to support the well-known Enterprise Integration Patterns.",
            "It enables lightweight messaging within Spring-based applications and supports integration with external systems via declarative adapters. ",
            " Those adapters provide a higher-level of abstraction over Spring’s support for remoting, messaging, and scheduling. ",
            "Spring Integration’s primary goal is to provide a simple model for building enterprise integration solutions while maintaining the separation of concerns that is essential for producing maintainable, testable code."
    };
    private Random random = new Random();
    private Map<UUID, String> cache = new HashMap<>();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        int index =random.nextInt(sentences.length);
        String sentence = sentences[index];
        UUID msgId = UUID.randomUUID();
        collector.emit(new Values(sentence), msgId);
        cache.put(msgId, sentence);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void ack(Object msgId) {
        cache.remove((UUID)msgId);
        System.out.println("MsgId=" + msgId + ", 处理成功。");
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("MsgId=" + msgId + ", 处理失败。");
        String sentence = cache.get((UUID)msgId);
        if(sentence != null)
            collector.emit(new Values(sentence), msgId);
    }
}
