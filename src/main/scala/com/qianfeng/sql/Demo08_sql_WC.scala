package com.qianfeng.sql

import com.qianfeng.common.{WordCount, WordCount1, YQTimeStamp}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.TableSource
import org.apache.flink.types.Row


/**
 * sql的实时词频统计
 */
object Demo08_sql_WC {
  def main(args: Array[String]): Unit = {
    //1、获取流式表执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //1、获取数据源
    val ds: DataStream[WordCount1] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .flatMap(_.split(" "))
      .map(x => {
        WordCount1(x, 1)
      })

    import org.apache.flink.table.api.scala._
    val table: Table = tenv.fromDataStream[WordCount1](ds)
    tenv.registerDataStream("t1",ds)
    //tenv.registerTableSource("t1", )
    //sql操作
    tenv.sqlQuery(
      s"""
         |select
         |word,
         |sum(cnt)
         |from $table
         |group by word
         |""".stripMargin)
        //.toAppendStream[Row]
        .toRetractStream[Row]
        .print("sql wc---")

    //触发
    env.execute("sql wc")
  }
}
