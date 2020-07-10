package com.qianfeng.batch

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
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
    val text: DataSet[String] = env.fromElements("i like flink", "flink", "like flink")

    /*//map:
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
    data.reduce(_+_).print()*/

    //aggregate： 对某个分组聚合，可以应用于某个完整数据集或者某个分组的数据集。
    println("aggregate=====")
    val data1: DataSet[(Int, String, Int)] = env.fromElements((1, "zs", 16), (1, "ls", 20), (2, "goudan", 23), (3, "mazi", 30))
    data1.aggregate(Aggregations.SUM,0).aggregate(Aggregations.MIN,2).print()
    //如上语句等价
    data1.sum(0).min(2).print()

    //minBy|maxBy : 取最大或者最小值
    data1.minBy(0).print()
    data1.minBy(0,2).print()

    //group by : 用来讲相同key进行分组
    data1.groupBy(0,1)
    //根据第一个字段进行分区，第3个字段进行升序排序
    data1.groupBy(0).sortGroup(2,Order.ASCENDING)
    data1.groupBy(0).sum(2).max(2).print()

    //join : 将两个dataset进行连接，生成新的dataset，默认使用inner join
    val d1: DataSet[(String, Int)] = env.fromElements(("a", 11), ("b", 22),("a", 66), ("a", 33))
    val d2: DataSet[(String, Int)] = env.fromElements(("a", 1), ("b", 2), ("c", 3))
    println("join-================")
    //where 左数据集字段
    d1.join(d2).where(0).equalTo(0).print()

    //union： 并集
    d1.union(d2).print()

    //cross : 笛卡尔积
    d1.cross(d2).print()

    //分区
    //reblance: 将数据集重新均衡分配，避免数据倾斜。后面仅能map操作
    println("rebalance================")
    text.rebalance().map(x=>(x,1)).print()
    text.rebalance().map(x=>(x,1)).partitionByHash(0).print()
    //范围分区、排序分区和自定义分区
    println("first================")
    //first-n： top  sortGroup : 分组排序(组内排序)
    text.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    /*.groupBy(0)
    .sortGroup(1,Order.DESCENDING)
    .first(2)
    .print()*/

    d1.groupBy(0).sortGroup(1,Order.DESCENDING).first(2).print()
  }
}
