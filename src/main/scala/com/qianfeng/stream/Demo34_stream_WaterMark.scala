package com.qianfeng.stream

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * flink的水印
 */
object Demo34_stream_WaterMark {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //附件时间 --- 事件时间类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //uname timestamp
    env.socketTextStream("hadoop01", 6666)
        .filter(_.nonEmpty)
        .map(x=>{
          val fileds: Array[String] = x.split(" ")
          (fileds(0).trim,fileds(1).toLong)
        })
        .assignTimestampsAndWatermarks(new MyWatermarkAssinger)
        .keyBy(0)
        .timeWindow(Time.seconds(3))
      //.apply()  为咯查看窗口和水印及触发相关信息
        .apply(new RichWindowFunction[(String,Long),String,Tuple,TimeWindow] {
          val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          //处理买一个窗口的信息
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
            val lst: List[(String, Long)] = input.iterator.toList.sortBy(_._2) //根据时间戳将窗口的数据进行排序
            println("=============")
            //根据排序的window获取开始数据和结束数据
            val startTime: Long = window.getStart
            val endTime: Long = window.getEnd
            val res = s"key->${key.getField(0)}," +
              s"事件开始时间EventTime->${fmt.format(lst.head._2)}," +
              s"事件结束时间EventTime->${fmt.format(lst.last._2)}," +
              s"窗口开始时间->${startTime}," +
              s"窗口结束时间->${endTime}"
          //返回字符串
            out.collect(res)
          }
        })
        .print()

    //触发执行
    env.execute("watermark")
  }
}

/*
AssignerWithPeriodicWatermarks[(String,Long)] : 周期性水印分配  --- 常用
AssignerWithPunctuatedWatermarks : 基于事件的水印分配
 */
class MyWatermarkAssinger extends AssignerWithPeriodicWatermarks[(String,Long)]{

  var maxTimestamp = 0L //迄今为止最大时间戳
  val lateness = 1000*10 //允许最大延迟数据的时间 10s

  val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  //获取当前水印
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimestamp - lateness)  //为咯延迟数据数据处理
  }

  //抽取时间戳
  override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
    //获取当前事件的时间戳
    val now_time = element._2
    maxTimestamp = Math.max(now_time,maxTimestamp) //用当前数据时间和最大时间戳计算最大时间错
    //方便测试打印观看---
    val now_watermark: Long = getCurrentWatermark.getTimestamp
    println(s"Event时间->${now_time} | ${fmt.format(now_time)}, " +
      s"本窗口迄今为止最大的时间->${maxTimestamp} | ${fmt.format(maxTimestamp)}," +
      s"当前watermark->${now_watermark} | ${fmt.format(now_watermark)}")
    //返回时间
    now_time
  }
}