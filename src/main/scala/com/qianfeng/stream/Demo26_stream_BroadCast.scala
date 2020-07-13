package com.qianfeng.stream


import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

/**
 * flink的广播变量
 * 1、广播数据为sex数据集 1 男  2 女  3
 * 2、实时流数据和广播的进行join操作，将sexid 编号变成对应的lab
 */
object Demo26_stream_BroadCast {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //广播数据
    val d1: DataStream[(Int, Char)] = env.fromElements((1, '男'), (2, '女'), (3, '妖'))
    //输入数据格式：uid uname genderFlag addr
    val d2: DataStream[(String, String, Int, String)] = env.socketTextStream("hadoop01", 6666)
      .map(line => {
        val fields: Array[String] = line.split(" ")
        (fields(0), fields(1), fields(2).toInt, fields(3))
      })

    d2.print("----")

    //广播d1出去
    val genderInfoDesc: MapStateDescriptor[Integer, Character] = new MapStateDescriptor(
      "genderInfo",
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.CHAR_TYPE_INFO
    )
    val bsStream: BroadcastStream[(Int, Char)] = d1.broadcast(genderInfoDesc)

    //使用广播变量
    d2.connect(bsStream)
        .process(new BroadcastProcessFunction[(String,String,Int,String),(Int,Char),(String,String,Char,String)] {
          //处理实时流中的每一行数据
          override def processElement(value: (String, String, Int, String),
                                      ctx: BroadcastProcessFunction[(String, String, Int, String),
                                        (Int, Char), (String, String, Char, String)]#ReadOnlyContext,
                                      out: Collector[(String, String, Char, String)]): Unit = {
            //获取广播变量状态
            val genderFlag = value._3
            var gender: Character = ctx.getBroadcastState(genderInfoDesc).get(genderFlag)
            //判断是否为空
            if(gender == null){
              gender = 'o'
            }
            //输出
            out.collect((value._1,value._2,gender,value._4))
          }

          //处理广播变量中的数据
          override def processBroadcastElement(value: (Int, Char),
                                               ctx: BroadcastProcessFunction[(String, String, Int, String),
                                                 (Int, Char), (String, String, Char, String)]#Context,
                                               out: Collector[(String, String, Char, String)]): Unit = {
            //将广播变量中数据写出 --- 共享
            ctx.getBroadcastState(genderInfoDesc).put(value._1,value._2)
          }
        })
        .print("broadcast ---")

    //5、触发执行  流应用一定要触发执行
    env.execute("broadcast---")
  }
}