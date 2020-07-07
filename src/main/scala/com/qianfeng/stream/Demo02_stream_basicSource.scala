package com.qianfeng.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * 流式的基础的source源
 */
object Demo02_stream_basicSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1、基于集合
    env.fromElements("i link flink","flink").print("fromElements-")
    //创建一个集合
    val list: ListBuffer[Int] = new ListBuffer[Int]()
    list += 10
    list += 20
    list += 33
    env.fromCollection(list).filter(x=>x>20).print("fromCollection-")
    println("collection-===================")

    //2、基于文件
    env.readTextFile("E:\\flinkdata\\words.txt","UTF-8").print("readTextFile-")
    env.readTextFile("hdfs://hadoop01:9000/words","UTF-8").print("readTextFile-")
    println("file-===================")

    //3、基于socket --- 不支持设置并行度
    env.socketTextStream("hadoop01",6666).setParallelism(1).print("")

    //触发执行
    env.execute("basic source")
  }
}
