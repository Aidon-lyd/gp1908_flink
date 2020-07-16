package com.qianfeng.sql

import com.qianfeng.common.YQTimeStamp
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row


/**
 * sql操作
 */
object Demo05_sql_simple {
  def main(args: Array[String]): Unit = {
    //1、获取流式表执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)


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
        val timestamp: Long = fields(3).trim.toLong
        YQTimeStamp(date, timestamp, province, add, possible)
      })

   //基于Dstream生成表
    //别名需要加入table.scal包  2、如果输入数据流为对象类型，则直接使用对象中的属性
    val table: Table = tenv.fromDataStream[YQTimeStamp](ds)

   //使用sql操作
    import org.apache.flink.table.api.scala._
    tenv.sqlQuery(
      s"""
         |select
         |*
         |from $table
         |where adds > 10
         |""".stripMargin)
        .toAppendStream[Row]
        .print("sql---")

    //触发
    env.execute("sql")
  }
}
