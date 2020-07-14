package com.qianfeng.stream

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

/**
 * 分布式缓存---
 */
object Demo27_stream_DistributedCache {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1、注册分布式缓存文件
    /**
     * 1 男
     * 2 女
     * 3 妖
     */
    env.registerCachedFile("hdfs://hadoop01:9000/flink_cache/sex","genderInfo")

    //输入数据格式：uid uname genderFlag addr
    env.socketTextStream("hadoop01", 6666)
      .map(new RichMapFunction[String,(String,String,Char,String)] {
        //定义存储缓存文件中的信息
        val map: mutable.HashMap[Int, Char] = mutable.HashMap()

        //定义用于存储缓存文件的数据流
        var bs:BufferedSource = _

        //打开缓存文件，执行一次
        override def open(parameters: Configuration): Unit = {
          //1、获取缓存文件  2、读缓存文件放到流中  3、将流数据放到map中
          val cacheFile: File = getRuntimeContext.getDistributedCache.getFile("genderInfo")
          //读文件
          bs = Source.fromFile(cacheFile)
          //循环bs
          val lines_list: List[String] = bs.getLines().toList
          for (line <- lines_list){
            val fields: Array[String] = line.split(" ")
            val genderFlag: Int = fields(0).trim.toInt
            val genderLab = fields(1).trim.toCharArray()(0)
            map.put(genderFlag,genderLab)
          }
        }

        //每行数据映射一次
        override def map(value: String): (String, String, Char, String) = {
          val fields: Array[String] = value.split(" ")
          val uid = fields(0).trim
          val uname =  fields(1).trim
          val genderFlag = fields(2).toInt
          val addr = fields(3)

          //使用genderFlag和map中的key来进行连接
          val genderLab: Char = map.getOrElse(genderFlag, 'o')
          //封装返回
          (uid,uname,genderLab,addr)
        }

        //关闭
        override def close(): Unit = {
          if(bs != null){
            bs.close()
          }
        }
      })
        .print("distributed cache file ---")

    //5、触发执行  流应用一定要触发执行
    env.execute("distributed cache file---")
  }
}