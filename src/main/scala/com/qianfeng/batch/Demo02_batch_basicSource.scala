package com.qianfeng.batch

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

import scala.collection.mutable.ListBuffer

/**
 * 批次的基础的source源
 */
object Demo02_batch_basicSource {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

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

    //3、基于通用 --- 需要自己写InputFormat
    //env.createInput(FileInputFormat[String])
    env.createInput(new TextInputFormat(new Path("hdfs://hadoop01:9000/words")))
  }
}
