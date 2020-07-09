package com.qianfeng.stream

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._


/**
 *流式基础的sink
 */
object Demo13_stream_sink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //定义kafka消费所需

    //val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val dstream: DataStream[(Int, Int)] = env.fromElements(Tuple2(200, 66), Tuple2(100, 65), Tuple2(200, 56), Tuple2(200, 666), Tuple2(100, 678))

    val res: DataStream[(Int, Int)] = dstream.keyBy(0)
      .reduce((kv1, kv2) => (kv1._1, kv1._2 + kv2._2))

    //基于文件
    res.print("print sink---")
    res.writeAsText("E:\\flinkdata\\out\\0800",WriteMode.OVERWRITE)
    res.writeAsText("hdfs://hadoop01:9000/08t/06",WriteMode.OVERWRITE)
    res.writeAsCsv("hdfs://hadoop01:9000/08t/07",WriteMode.OVERWRITE)

    //socket写出数据任然只支持1个并行度
    //res.writeToSocket("hadoop01",9999,new SimpleStringSchema())  //---自行测试
    //触发执行
    env.execute("基础sink---")
  }
}
