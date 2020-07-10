package com.qianfeng.stream

import com.qianfeng.common.YQDetail
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}


/**
 *
 * 原始数据：
*date province add possible
*2020-7-1 beijing 1 2
*2020-7-2 beijing 2 1
*2020-7-3 beijing 1 0
*2020-7-3 tianjin 2 1
 **
 *需求：
*2、将如上明细数据打入到hdfs中
 */
object Demo19_stream_FileSink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //开启checkpoint
    env.enableCheckpointing(10*1000)

    /*//------------------行编码回滚
    val res: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //将其添加到sink中
    val outputPath: Path = new Path("hdfs://hadoop01:9000/out/flink/streamingfile/")
    //回滚策略
    val rollingPolicy: DefaultRollingPolicy[String, String] = DefaultRollingPolicy.create()
      .withRolloverInterval(10 * 1000) //落地间隔，，单位毫秒
      .withInactivityInterval(10 * 1000) //无数据交互落地间隔，，主要节约开销
      .withMaxPartSize(128 * 1024 * 1024) //超过该值落地
      .build()

    //数据桶分配器
    val bucketAssinger: DateTimeBucketAssigner[String] = new DateTimeBucketAssigner[String]("yyyyMMddHH")

    //获取FileSink
    val fileSink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(outputPath, new SimpleStringEncoder[String]("UTF-8")) //行编码
      .withBucketAssigner(bucketAssinger)
      .withRollingPolicy(rollingPolicy)
      .withBucketCheckInterval(10 * 1000) //桶检测间隔
      .build()
    //添加sink
    res.addSink(fileSink)*/

    /*
    1、落地按照天分区落地 /项目/表/dt=yyyyMMdd
    2、落地格式为parquet
     */
    try {
      //------------------块编码回滚
      val res: DataStream[YQDetail] = env.socketTextStream("hadoop01", 6666).map(x => {
        val fileds: Array[String] = x.split(" ")
        YQDetail(fileds(0), fileds(1), fileds(2).toInt, fileds(3).toInt)
      })
      res.print("============")

      //将其添加到sink中
      val outputPath: Path = new Path("hdfs://hadoop01:9000/out/flink/yq/dt=2020-07-10")

      //数据桶分配器
      val bucketAssinger: BasePathBucketAssigner[YQDetail] = new BasePathBucketAssigner()

      //获取FileSink
      val parquetFileSink: StreamingFileSink[YQDetail] = StreamingFileSink
        //块编码
        .forBulkFormat(
          outputPath,
          ParquetAvroWriters.forReflectRecord(classOf[YQDetail])) //指定使用parquet序列化
        .withBucketAssigner(bucketAssinger)
        .withBucketCheckInterval(10 * 1000) //桶检测间隔
        .build()
      //添加sink
      res.addSink(parquetFileSink)
    } catch {
      case e:Exception => e.printStackTrace()
    }

    //触发执行
    env.execute("file sink connector---")
  }
}
