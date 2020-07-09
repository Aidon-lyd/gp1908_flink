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
object Demo10_stream_Connect {
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
    val execption: DataStream[TempInfo] = splitStream.select("异常")

    //使用union进行合并
    val connectedStream: ConnectedStreams[TempInfo, TempInfo] = common.connect(execption)
    connectedStream.map(
      common1 => ("正常旅客id:"+common1.uid+" 姓名:"+common1.uname),
      execption1 => ("异常旅客id:"+execption1.uid+" 姓名:"+execption1.uname+" 温度:"+execption1.Temp)
    ).print("connected1 ---")

    //不同类型的合并
    val c1: DataStream[(Int, String)] = splitStream.select("正常").map(person => {
      (person.uid, person.uname)
    })
    val e1: DataStream[(Int, String, Double)] = splitStream.select("异常").map(person => {
      (person.uid, person.uname, person.Temp)
    })

    val res1: ConnectedStreams[(Int, String), (Int, String, Double)] = c1.connect(e1)
    res1.map(
      common1 => ("正常旅客id:"+common1._1+" 姓名:"+common1._2),
      execption1 => ("异常旅客id:"+execption1._1+" 姓名:"+execption1._2+" 温度:"+execption1._3)
    ).print("connected2 ---")
    //c1.union(e1)  //不同类型不能合并

    //触发执行
    env.execute("connected---")
  }
}
