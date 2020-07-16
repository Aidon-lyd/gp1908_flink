package com.qianfeng.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row


/**
 * 批次的表操作
 */
object Demo04_table_batch {
  def main(args: Array[String]): Unit = {
    //1、获取批次表执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tenv: BatchTableEnvironment = BatchTableEnvironment.create(env)

   //获取数据
    val ds: DataSet[(String,Int)] = env.readCsvFile("E:\\flinkdata\\test.csv")

    //dset转换成表
    import org.apache.flink.table.api.scala._
    val table: Table = tenv.fromDataSet(ds, 'name, 'age)

    //table操作
    table
      .groupBy('name)
      .select('name,'age.sum as 'sum_ages)
      .toDataSet[Row]
      .print()
  }
}
