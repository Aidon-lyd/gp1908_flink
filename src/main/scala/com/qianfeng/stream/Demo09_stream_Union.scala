package com.qianfeng.stream

import com.qianfeng.common.TempInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._


/**
 * Union和Connect： 合并流
 *
 * union：DataStream* → DataStream
 * 1、是将两个或者多个流进行合并，形成新的数据流；
 * 2、union的多个子流的类型需要一致
 *
 * connect：DataStream,DataStream → ConnectedStreams
 * 1、connect只能连接两个stream；
 * 2、被连接的两个子流的类型可以不一致
 * 3、被连接的两个流之间可以进行状态数据的共享，一个流的结果会影响另外一个流的结果，通常用于做累加非常用
 */
object Demo09_stream_Union {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //定义kafka消费所需

    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    //3、基于datastream转化 --- transformation
    //输入数据格式：uid uname temp timestamp location
    val splitStream: SplitStream[TempInfo] = dstream.map(personInfo => {
      val fields: Array[String] = personInfo.split(" ")
      TempInfo(fields(0).trim.toInt, fields(1).trim, fields(2).trim.toDouble, fields(3).trim.toLong, fields(4).trim)
    })
      .split((temp: TempInfo) => if (temp.Temp >= 36.0 && temp.Temp <= 37.8) Seq("正常") else Seq("异常"))

    //select选择流
    val common: DataStream[TempInfo] = splitStream.select("正常")
    val common1: DataStream[TempInfo] = splitStream.select("正常")
    val execption: DataStream[TempInfo] = splitStream.select("异常")

    //使用union进行合并
    common.union(execption,common1).print("union 合并流---")

    //触发执行
    env.execute("union---")
  }
}
