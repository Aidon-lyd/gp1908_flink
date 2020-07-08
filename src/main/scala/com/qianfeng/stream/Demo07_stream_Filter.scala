package com.qianfeng.stream

import java.util.Properties

import com.qianfeng.common.WordCount
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._


/**
 *flatmap|map|filter
 */
object Demo07_stream_Filter {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //定义kafka消费所需

    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    //3、基于datastream转化 --- transformation
    val res: DataStream[WordCount] = dstream.flatMap(_.split(" "))
      .filter(_.length > 5) //返回true留下
      .map(word => WordCount(word, 1)) //(hello,1)
      .keyBy("word") //分组，类似group by
      .sum("count")

    //持久化
    res.print("Filter-")

    //触发执行
    env.execute("Filter-")
  }
}
