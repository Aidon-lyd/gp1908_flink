package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner

/**
 * 控制操作链
 */
object Demo21_stream_chain {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //将map和print连接到一块
    env.fromElements("i like flink").map((_,1)).startNewChain().print("--startNewChain")

    //禁止连接map操作
    env.fromElements("i like flink").map((_,1)).disableChaining().print("--startNewChain")

    //共享slot --- map共享slot
    env.fromElements("i like flink").map((_,1)).slotSharingGroup("default")
    //5、触发执行  流应用一定要触发执行
    env.execute("operter chain---")
  }
}
