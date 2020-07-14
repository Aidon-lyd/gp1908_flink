package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * flink的常见window
 */
object Demo29_stream_Window {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //附件时间 --- 处理时间类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.socketTextStream("hadoop01", 6666)
        .flatMap(_.split(" "))
        .map((_,1))
        .keyBy(0)
        //.timeWindow(Time.seconds(5))  //基于时间滚动窗口
        //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
        //.timeWindow(Time.seconds(5),Time.seconds(2))  //基于时间滑动窗口
        //.countWindow(3) //基于数据滚动窗口
        //.countWindow(5,2) //基于数据的滑动窗口
        //.window(EventTimeSessionWindows.withGap(Time.seconds(5))) //基于时间的session窗口
        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
        .sum(1)
        .print("window---")
    //触发执行
    env.execute("window")
  }
}