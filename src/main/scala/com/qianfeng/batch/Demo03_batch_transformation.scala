package com.qianfeng.batch

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

import scala.collection.mutable.ListBuffer

/**
 * 批次的基础的常见算子
 */
object Demo03_batch_transformation {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    val text: DataSet[String] = env.fromElements("i like flink", "flink", "flink")

    //map:
    text.map((_,1)).print()

    //flatmap
    text.flatMap(_.split(" ")).print()

    //mappartition: 处理某个分区中的所有元素(“迭代器里面”)
    text.mapPartition(x=> x map((_,1))).print()

    //filter:
    text.filter(x=>x.length>10).print()

    //distinct : 去重,,,可以根据某字段去重
    text.distinct().print()
    text.flatMap(x=>x.split(" ")).map((_,1)).distinct(0).print()

    //reduce聚合：将整个DataSet数据进行合并
    val data: DataSet[Int] = env.fromElements(11, 22, 33)
    data.reduce(_+_).print()

  }
}
