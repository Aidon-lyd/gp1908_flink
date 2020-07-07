package com.qianfeng.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
/**
 *scala版本的批次词频统计
 */
object Demo01_batchWC {
  def main(args: Array[String]): Unit = {
    //1、获取批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2、获取数据源 source
    /*val dset: DataSet[String] = env.fromElements("i link flink and flink is nice", "flink if good nice")
    //3、转换
    val res: AggregateDataSet[(String, Int)] = dset
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //4、持久化   sink
    res.print()*/

    //读文件，，，写文件
    env.readTextFile(args(0),"UTF-8")
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .writeAsText(args(1)).setParallelism(1)

    //批次不需要触发执行 --- 如果触发将会报错为没有可执行的操作
    env.execute()
  }
}
