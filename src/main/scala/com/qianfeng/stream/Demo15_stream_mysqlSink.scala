package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import com.qianfeng.common.YQ
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


/**
 * SinkFunction:
 * RichSinkFunction:
原始数据：
date province add possible
2020-7-1 beijing 1 2
2020-7-2 beijing 2 1
2020-7-3 beijing 1 0
2020-7-3 tianjin 2 1

需求：
1、算出每天、省份的adds、possible
2、将如上计算结果打入到mysql中
 */
object Demo15_stream_mysqlSink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //定义kafka消费所需

    val res: DataStream[YQ] = env.socketTextStream("hadoop01", 6666)
      .map(line => {
        //构造key-value数据
        val fields: Array[String] = line.split(" ")
        //key： date_province value：(add possible)
        val date: String = fields(0).trim
        val province: String = fields(1).trim
        val add: Int = fields(2).trim.toInt
        val possible: Int = fields(3).trim.toInt
        (date + "_" + province, (add, possible))
      })
      .keyBy(0)
      .reduce((kv1, kv2) => (kv1._1, (kv1._2._1 + kv2._2._1, kv1._2._2 + kv2._2._2)))
      .map(y => {
        val date_province: Array[String] = y._1.split("_")
        new YQ(date_province(0), date_province(1), y._2._1, y._2._2)
      })
    //将res结果打入mysql中
    res.addSink(new MyMysqlSink)
    //触发执行
    env.execute("基础sink---")
  }
}

//自定义输出需要实现RichSinkFunction;;泛型是输入的类型
class MyMysqlSink extends RichSinkFunction[YQ]{
  //连接数据库的对象
  var conn:Connection = _
  var ps:PreparedStatement = _

  //初始化mysql的连接信息
  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop01:3306/test"
    val user = "root"
    val pass = "root"
    //反射
    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, user, pass)
    } catch {
      case e:SQLException => e.printStackTrace()
    }
  }

  //将结果数据打入的持久化中（每条记录执行一次）
  override def invoke(yq: YQ, context: SinkFunction.Context[_]): Unit = {
    ps = conn.prepareStatement("replace into yq(dt,province,adds,possibles) values(?,?,?,?)")
    //赋值
    ps.setString(1,yq.date)
    ps.setString(2,yq.province)
    ps.setInt(3,yq.adds)
    ps.setInt(4,yq.possibles)
    //执行插入
    ps.execute()
  }

  //关闭
  override def close(): Unit = {
    if(ps != null){
      ps.close()
    }
    if(conn != null){
      conn.close()
    }
  }
}
