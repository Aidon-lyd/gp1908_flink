package com.qianfeng.stream

import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * flink的checkpoint设置
 */
object Demo24_stream_Checkpoint {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //checkpoint开启
    env.enableCheckpointing(10*1000,CheckpointingMode.AT_LEAST_ONCE)  //开启checkpoint

    //配置其它checkpoint相关参数
    val config: CheckpointConfig = env.getCheckpointConfig
    /*
    RETAIN_ON_CANCELLATION ： 取消作业时，保留checkpoint数据
    DELETE_ON_CANCELLATION: 取消作业时，删除checkpoint数据
     */
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    config.setCheckpointTimeout(30*1000) //超时设置
    config.setMaxConcurrentCheckpoints(1) //同时运行多少个检查点进行

    //状态后端存储
    //有3种存储：
    // 1、内存
    // 2、hdfs
    // 3、rocksDB
    env.setStateBackend(new MemoryStateBackend(128*1024*1024,true))
    //env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/1908_checkpoint"))
    //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop01:9000/flink/1908_checkpoint",true))

    // 假设我们有一个 StreamExecutionEnvironment类型的变量 env
    import org.apache.flink.api.scala._
    env.socketTextStream("hadoop01",6666)
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()
    //5、触发执行  流应用一定要触发执行
    env.execute("checkpoint---")
  }
}