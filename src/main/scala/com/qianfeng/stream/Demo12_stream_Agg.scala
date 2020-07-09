package com.qianfeng.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._


/**
 *AGG聚合：
 *keyedStream.sum(0)
 *keyedStream.sum("key")
 *keyedStream.min(0)
 *keyedStream.min("key")
 *keyedStream.max(0)
 *keyedStream.max("key")
 *keyedStream.minBy(0)
 *keyedStream.minBy("key")
 *keyedStream.maxBy(0)
 *keyedStream.maxBy("key")
 */
object Demo12_stream_Agg {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //定义kafka消费所需

    //val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val dstream: DataStream[(Int, Int)] = env.fromElements(Tuple2(200, 66), Tuple2(100, 65), Tuple2(200, 56), Tuple2(200, 666), Tuple2(100, 678))

    val keyedStream: KeyedStream[(Int, Int), Tuple] = dstream.keyBy(0)
    //聚合算子
    //keyedStream.sum(1).print("sum-")
    keyedStream.min(1).print("min-")
    //keyedStream.minBy(1).print("minBy-")
   /* keyedStream.max(1).print("max-")
    keyedStream.maxBy(1).print("maxBy-")*/

    //触发执行
    env.execute("agg---")
  }
}
