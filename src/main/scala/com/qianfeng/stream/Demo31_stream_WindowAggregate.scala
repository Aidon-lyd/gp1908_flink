package com.qianfeng.stream

import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * window的reduce聚合
 * 某天某省每5秒平均新增
 */
object Demo31_stream_WindowAggregate {
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
        .aggregate(new AggregateFunction[(String,Int),(String,Int,Int),(String,Double)] {
          //累加器初始化
          override def createAccumulator(): (String, Int, Int) = ("",0,0)

          //单个累加器的相加
          override def add(value: (String, Int), accumulator: (String, Int, Int)): (String, Int, Int) = {
            //输入条数
            val cnt = accumulator._2 + 1
            val adds = accumulator._3 + value._2 //累加新增
            (value._1,cnt,adds)
          }

          //多个累加器的相加
          override def merge(a: (String, Int, Int), b: (String, Int, Int)): (String, Int, Int) = {
            val add_cnt = a._2 + b._2
            val adds= a._3 + b._3
            (a._1,add_cnt,adds)
          }

          //返回结果
          override def getResult(accumulator: (String, Int, Int)): (String, Double) = {
            (accumulator._1,accumulator._3*1.0/accumulator._2)
          }
        })
        .print("window agg---")

    //触发执行
    env.execute("window agg")
  }
}