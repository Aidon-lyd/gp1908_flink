package com.qianfeng.stream

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * flink的valuestate
 */
object Demo23_stream_ValueState {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 假设我们有一个 StreamExecutionEnvironment类型的变量 env
    import org.apache.flink.api.scala._
    env.fromElements((1L, 5L), (1L, 6L), (1L, 10L), (2L, 3L), (2L, 8L))
      .keyBy(0)
      .flatMap(new MyFlatMapFunction())
      .print()
    //5、触发执行  流应用一定要触发执行
    env.execute("valuestate---")
  }
}

//自定义函数，，---并使用valuestate
/**
 * 自定义state管理
 * 0、每个算子在源码中都有对应的*Function和Rich*Function
 * 1、继承对应函数的富函数（每个对应函数的富函数都有继承抽象的富函数:AbstractRichFunction）
 * 2、实现对应的函数即可
 *
 * 需求：
 * 1、不能使用reduce、sum等聚合，实现相同key的值进行累加的聚合
 * 2、或者实现每两个key的平均值
 */
class MyFlatMapFunction extends RichFlatMapFunction[(Long,Long),(Long,Long)]{
  /**
   * 初始化用于存储状态的熟悉，key：是count，value：是sum值
   */
  var sum:ValueState[(Long,Long)] = _

  //初始化状态
  override def open(parameters: Configuration): Unit = {
    //初始化valuestate描述器
    val descriptor: ValueStateDescriptor[(Long, Long)] = new ValueStateDescriptor[(Long, Long)](
      "average",
      TypeInformation.of(new TypeHint[(Long, Long)] {}), //---可以使用createTypeInfomation()
      (0l, 0l)) //给当前状态一个初始值
    //获取其状态
    sum = getRuntimeContext().getState(descriptor)
  }

  //每一条数据执行一次该方法
  /**
   *
   * @param value
   * @param out
   *
   *            (1L, 5L), (1L, 6L), (1L, 10L), (2L, 3L), (2L, 8L)
   *            :
   *            第一次：1 5
   *            第二次：1 11
   *            第三次：1 21
   *            第四次：2 3
   */
  override def flatMap(value: (Long, Long), out: Collector[(Long, Long)]): Unit = {
    //获取当前状态
    var currentSum: (Long, Long) = sum.value()

    //count + 1
    val count = currentSum._1 + 1
    //求sum
    val sumed = currentSum._2 + value._2

    // 更新状态
    sum.update((count,sumed))

    /*//输出状态
    out.collect(value._1,sumed)*/

    //状态输出：如果count到达2, 保存 count和平均值，清除之前的状态
    if (sum.value()._1 >= 2) {
      out.collect((value._1, sum.value()._2 / sum.value()._1)) //1 5
      //状态清空
      sum.clear()
    }
  }
}