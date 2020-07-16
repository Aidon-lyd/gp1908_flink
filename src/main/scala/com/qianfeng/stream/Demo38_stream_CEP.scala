package com.qianfeng.stream

import com.qianfeng.common.{Event, SubEvent}
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * flink的CEP操作
 * 1、匹配start->*->middle-*->end  (匹配过可以再次匹配)
 * 2、匹配start->middle->*->end   (宽松近邻)
 * 3、匹配start->middle->end   (严格近邻)
 * 4、匹配start->*->middle-*->end  (匹配过可以再次匹配) ---middle出现2次的
 */
object Demo38_stream_CEP {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //数据源获取
    val ds: DataStream[Event] = env.fromElements(
      new Event(1, "start", 1.0),
      new Event(2, "middle", 2.0),
      new Event(3, "foobar", 3.0),
      new Event(4, "foo", 4.0),
      new Event(5, "middle", 5.0),
      new SubEvent(6, "middle", 6.0, 100),
      new SubEvent(7, "bar", 7.0, 1000),
      new Event(66, "666", 66.0),
      new Event(8, "end", 8.9)
    )

    //模式定义 --- pattern 导scala包中，，注意引入依赖
    val pattern: Pattern[Event, Event] = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))
      .followedByAny("middle")
      .where(_.getName.equals("middle"))
      .followedByAny("end")
      .where(_.getName.equals("end"))

    //模式检测
    val ps: PatternStream[Event] = CEP.pattern(ds, pattern)

    //3、符合规则数据的获取
    ps.flatSelect[String]((ele:scala.collection.Map[String,Iterable[Event]],out:Collector[String])=>{
      //定义一个用于存储符合数据对象
      val builder: StringBuilder = new StringBuilder
      //取出匹配的数据
      val startEvent: String = ele.get("start").get.toList.head.toString
      val middleEvent: String = ele.get("middle").get.toList.head.toString
      val endEvent: String = ele.get("end").get.toList.head.toString

      //存储到buiddler
      builder.append(startEvent).append("\t|\t")
        .append(middleEvent).append("\t|\t")
        .append(endEvent)

      //将匹配的上结果输出
      out.collect(builder.toString())
    })
        .print("cep start->middle->end")

    //触发执行
    env.execute("cep")
  }
}
