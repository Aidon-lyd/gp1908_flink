package com.qianfeng.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._


/**
 *使用flink-kafka的连接器消费kafka的数据
 */
object Demo06_stream_KafkaConnector {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10)  //开启checkpoint
    //定义kafka消费所需
    val from_topic = "test"
    val pro: Properties = new Properties()
    //使用类加载器加载consumer.properties
    pro.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    //添加flinkkafka的消费者
    //1.0及以后的kafka的通用依赖
    //val res: DataStream[String] = env.addSource(new FlinkKafkaConsumer(from_topic, new SimpleStringSchema(), pro))

    //0.9的kafka的代码
    val flinkKafkaConsumer: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09(from_topic, new SimpleStringSchema(), pro)
    //设置消费相关信息
    flinkKafkaConsumer.setStartFromLatest()  //从最新的位置消费
    flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true) //设置offset提交基于checkpoint

    val res: DataStream[String] = env.addSource(flinkKafkaConsumer)


    //持久化
    res.print("flink-kafka-")

    //触发执行
    env.execute("kafka connector-")
  }
}
