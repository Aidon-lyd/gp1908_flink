package com.qianfeng.stream

import java.util.Properties

import com.qianfeng.common.{YQ, YQSchema}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic


/**
 *
原始数据：
date province add possible
2020-7-1 beijing 1 2
2020-7-2 beijing 2 1
2020-7-3 beijing 1 0
2020-7-3 tianjin 2 1

需求：
1、算出每天、省份的adds、possible
2、将如上计算结果打入到kafka中
 */
object Demo16_stream_KafkaSink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //定义kafka消费所需
    val res: DataStream[YQ] = env.socketTextStream("hadoop01", 6666)
      .map(line => {
        //构造key-value数据
        val fields: Array[String] = line.split(" ")
        //key： date_province value：(add possible)
        val date: String = fields(0).trim
        val province: String = fields(1).trim
        val add: Int = fields(2).trim.toInt
        val possible: Int = fields(3).trim.toInt
        (date + "_" + province, (add, possible))
      })
      .keyBy(0)
      .reduce((kv1, kv2) => (kv1._1, (kv1._2._1 + kv2._2._1, kv1._2._2 + kv2._2._2)))
      .map(y => {
        val date_province: Array[String] = y._1.split("_")
        new YQ(date_province(0), date_province(1), y._2._1, y._2._2)
      })

    //将res结果打入kafka中 --- broker_list totopic 序列化
    val broker_list = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    val to_topic = "yq_report"
    val pro: Properties = new Properties()
    pro.put("bootstrap.servers",broker_list)
    //必须要设置语义
    val flinkKafkaProducer: FlinkKafkaProducer[YQ] = new FlinkKafkaProducer[YQ](
      to_topic,
      new YQSchema(to_topic),
      pro,
      Semantic.EXACTLY_ONCE  //正好一次语义
      /**
       * NONE:容易因为算子失败或者整个任务失败，会出现数据重复或者丢失可能。不建议使用
       * AT_LEAST_ONCE ： 至少一次，，flinkKafkaProducer默认使用该语义。保障数据不会丢，但是可能出现重复。
       * EXACTLY_ONCE ： 正好一次，保障数据不丢失，不重复
       */
    )
    //设置生产者相关属性
    flinkKafkaProducer.setWriteTimestampToKafka(true)
    //将其添加到sink中
    res.addSink(flinkKafkaProducer)
    //触发执行
    env.execute("kafka sink connector---")
  }
}
