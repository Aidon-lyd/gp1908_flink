package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner

/**
 * flink的物理分区
 * 1、可以解决数据倾斜问题
 * 2、可以解决小表关联大表优化问题
 */
object Demo22_stream_partitioner {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //shuffle---将上游的数据随机发送到下游(随机选择channel)
    dstream.shuffle.print("shuffle---").setParallelism(4)
    //rebalance --- 负载均衡，轮询发送---
    dstream.rebalance.print("reblance---").setParallelism(4)
    //.rescale 扩扩展分区，，也是使用轮询---和rebalance不一样
    dstream.rescale.print("rescale---").setParallelism(4)
    //.broadcast --- 广播分区，，一般上游数据要小
    dstream.broadcast.print("broadcast---").setParallelism(4)
    //自定义分区器： dataStream.partitionCustom(partitioner, "someKey")

    //5、触发执行  流应用一定要触发执行
    env.execute("partitioner---")
  }
}
