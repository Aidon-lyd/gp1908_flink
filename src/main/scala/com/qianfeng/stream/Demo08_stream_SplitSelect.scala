package com.qianfeng.stream

import com.qianfeng.common.{TempInfo, WordCount}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._


/**
 *Split|select
 * split:
 * 拆分流，也就是将一个DataStream拆分成多个SplitStream
 *
 * select:SplitStream--->DataStream
 * 跟split搭配使用，先拆分，然后再选择对应的流
 *
 */
object Demo08_stream_SplitSelect {
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
    splitStream.select("正常").print("正常旅客---")
    splitStream.select("异常").print("异常旅客---")

    //触发执行
    env.execute("split select-")
  }
}
