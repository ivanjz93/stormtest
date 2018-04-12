package org.dan.order;


import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * 生成订单的log日志
 */
public class OrderWriter {

    public static final String TOPIC = "test12345";
    public static void main(String[] args) throws Exception {
        String logFilePath;
        if(args.length < 1)
            logFilePath = "/tmp/order.log";
        else
            logFilePath = args[0];
        PrintWriter pw = new PrintWriter(new FileWriter(logFilePath));

        for(int i = 0; i < 100; i++) {
            String orderInfoStr = new OrderInfo().random();
            pw.println(orderInfoStr);
            Thread.sleep(1000);
        }
        pw.close();
    }
}
