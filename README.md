Storm的一些练习：

- org.dan.wordcount: 经典单词计数；
- org.dan.ackfail: storm的ack fail机制；
- org.dan.mystorm: 仿storm框架简单实现wordcount（业务代码与框架代码耦合）。
- org.dan.order: 订单金额实时计算  OrderWriter->log文件->flume->kafka->storm->redis或OrderMqSender->kafka->storm->redis