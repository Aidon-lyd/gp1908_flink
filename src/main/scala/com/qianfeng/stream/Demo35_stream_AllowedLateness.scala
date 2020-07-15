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
 * flink的允许迟到数据
 */
object Demo35_stream_AllowedLateness {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //如果想看触发效果，，需要设置1个并行度
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
        .allowedLateness(Time.seconds(2))  //允许迟到2秒
      //.apply()  为咯查看窗口和水印及触发相关信息
        .apply(new RichWindowFunction[(String,Long),String,Tuple,TimeWindow] {
          val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          //处理买一个窗口的信息
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
            val lst: List[(String, Long)] = input.iterator.toList.sortBy(_._2) //根据时间戳将窗口的数据进行排序
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