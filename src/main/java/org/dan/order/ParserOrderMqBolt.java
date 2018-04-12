package org.dan.order;

import com.google.gson.Gson;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

public class ParserOrderMqBolt extends BaseRichBolt {

    private JedisPool pool;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        pool = new JedisPool("nn1", 6379);
    }

    @Override
    public void execute(Tuple input) {

        String str = (String)input.getValue(0);

        OrderInfo orderInfo = (OrderInfo)new Gson().fromJson(str, OrderInfo.class);
        //整个网站，各个业务线，各个品类，各个店铺，各个品牌，每个商品
        Jedis jedis = pool.getResource();
        //获取整个网站的金额统计指标
        jedis.incrBy("totalAmount1", orderInfo.getProductPrice());
        //获取商品所属业务线的指标信息
        String bid = getBuByProductId(orderInfo.getProductId(), "b");
        jedis.incrBy(bid + "Amount1", orderInfo.getProductPrice());
        System.out.println("------" + jedis.get(bid+"Amount1"));
        jedis.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private String getBuByProductId(String productId, String type) {

        //productID ----> <各个业务线，各个品类，各个店铺，各个品牌，每个商品>
        //从redis获取商品所属的业务单元编号
        //key:value
        //productID:map


        Map<String, String> map = new HashMap<>();
        map.put("b", "3c");
        map.put("c", "phone");
        map.put("s", "121");
        map.put("p", "apple");
        return map.get(type);
    }
}
