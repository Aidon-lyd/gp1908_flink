package com.qianfeng.stream

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.lucene.index.TwoPhaseCommit

/**
 * window的reduce聚合
 * 某天某省 近5秒平均新增
 */
object Demo32_stream_WindowProcess {
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
        .timeWindow(Time.seconds(10),Time.seconds(5))
        .process[(String,Int)](new ProcessWindowFunction[(String,Int),(String,Int),Tuple,TimeWindow]{
          //处理窗口中每一条数据(放到迭代器中)
          override def process(key: Tuple, context: Context,
                               elements: Iterable[(String, Int)],
                               out: Collector[(String, Int)]): Unit = {
            //cnt计算
            var cnt = 0
            var adds = 0
            //循环迭代器
            elements.foreach(line=>{
              cnt = cnt + 1
              adds = adds + line._2
            })
            //输出
            out.collect((key.getField(0),adds/cnt))
          }
        })
        .print("window process---")

    //触发执行
    env.execute("window process")
  }
}