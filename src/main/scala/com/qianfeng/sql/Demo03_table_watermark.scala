package com.qianfeng.sql

import com.qianfeng.common.YQTimeStamp
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row


/**
 * 水印
 */
object Demo03_table_watermark {
  def main(args: Array[String]): Unit = {
    //1、获取流式表执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //使用水印--
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1、获取数据源
    val ds: DataStream[YQTimeStamp] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .map(line => {
        //构造key-value数据
        val fields: Array[String] = line.split(" ")
        //key： date_province value：(add possible)
        val date: String = fields(0).trim
        val province: String = fields(1).trim
        val add: Int = fields(2).trim.toInt
        val possible: Int = fields(3).trim.toInt
        val timestamp: Long = fields(4).trim.toLong
        YQTimeStamp(date, timestamp, province, add, possible)
      })
      //设置允许最大乱序数据时长 2
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[YQTimeStamp](Time.seconds(2)) {
        //抽取时间戳
        override def extractTimestamp(element: YQTimeStamp): Long = element.timestamp  //保障输入时间戳为毫秒
      })

   //基于Dstream生成表
    //别名需要加入table.scal包   2、别名不能使用单引号和双引号
    import org.apache.flink.table.api.scala._
    //.rowtime是Eventtime   .processtime：是系统处理时间
    val table: Table = tenv.fromDataStream(ds,'dt,'timestamp.rowtime)


    //基于表的操作
    //Tumble over 2.second --- 滚动窗口，大小为2
    //on 'timestamp --- 窗口基于事件时间
    //as 'tt --- 窗口别名
    val table1: Table = table
      .window(Tumble over 2.second on 'timestamp as 'tt)
      .groupBy('dt, 'tt)
      .select('dt, 'dt.count)

    //将表转换成流 1、通过tenv转换  2、通过table中转换  3、注意泛型为Row
    tenv.toAppendStream[Row](table1).print("window和水印,每隔2秒的上报数据次数---")

    //触发
    env.execute("window watermark")
  }
}
