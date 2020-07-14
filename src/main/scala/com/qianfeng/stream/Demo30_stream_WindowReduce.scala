package com.qianfeng.stream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * window的reduce聚合
 * 某天某省每5秒新增最大---
 */
object Demo30_stream_WindowReduce {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //date province add
    env.socketTextStream("hadoop01", 6666)
      .map(line => {
        //构造key-value数据
        val fields: Array[String] = line.split(" ")
        //key： date_province value：(add possible)
        val date: String = fields(0).trim
        val province: String = fields(1).trim
        val add: Int = fields(2).trim.toInt
        (date + "_" + province, add)
      })
      .keyBy(0)
        .timeWindow(Time.seconds(5))
        .reduce(new ReduceFunction[(String, Int)] {
          //聚合
          override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
            //比较找最大值
            if(value1._2 > value2._2){
              value1
            } else {
              value2
            }
          }
        })
        .print("window reduce---")

    //触发执行
    env.execute("window reduce")
  }
}