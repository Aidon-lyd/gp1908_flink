package com.qianfeng.sql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row


/**
 * 表操作
 */
object Demo01_table_simapleApi {
  def main(args: Array[String]): Unit = {
    //1、获取流式表执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    //1、获取数据源
    val ds: DataStream[(String, String, Int, Int)] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .map(line => {
        //构造key-value数据
        val fields: Array[String] = line.split(" ")
        //key： date_province value：(add possible)
        val date: String = fields(0).trim
        val province: String = fields(1).trim
        val add: Int = fields(2).trim.toInt
        val possible: Int = fields(3).trim.toInt
        (date, province, add, possible)
      })

   //基于Dstream生成表
    val table: Table = tenv.fromDataStream(ds)

    //基于表的操作
    //val table1: Table = table.select("_1,_2,_3")
    val table1: Table = table.select("_1,_2,_3")
        .where("_3>10")

    //将表转换成流 1、通过tenv转换  2、通过table中转换  3、注意泛型为Row
    tenv.toAppendStream[Row](table1)
    .print("table---")

    //触发
    env.execute("table")
  }
}
