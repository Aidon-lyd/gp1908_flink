package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import com.qianfeng.common.Stu
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
 * 读取mysql的数据
 */
object Demo04_stream_mysqlSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //添加源
    val dstream: DataStream[Stu] = env.addSource(new MyMysqlSource)
    //打印
    dstream.print("my MyMysqlSource-")

    //触发执行
    env.execute("MyMysqlSource source")
  }
}

/*
CREATE TABLE `stu1` (
  `id` int(11) DEFAULT NULL,
  `name` varchar(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

1、泛型为输出的数据类型
 */
class MyMysqlSource() extends RichSourceFunction[Stu]{
  //连接数据库的对象
  var conn:Connection = _
  var ps:PreparedStatement = _
  var rs:ResultSet = _

  //用于初始化
  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop01:3306/test"
    val user = "root"
    val pass = "root"
    //反射
    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, user, pass)
      //查询
      ps = conn.prepareStatement("select * from stu1")
    } catch {
      case e:SQLException => e.printStackTrace()
    }
  }

  //往下游打数据
  override def run(ctx: SourceFunction.SourceContext[Stu]): Unit = {
    //获取rs中的数据
    try {
      rs = ps.executeQuery()
      while (rs.next()) {
        val stu: Stu = new Stu(rs.getInt(1), rs.getString(2).trim)
        //输出
        ctx.collect(stu)
      }
    } catch {
      case e:SQLException => e.printStackTrace()
    }
  }

  override def cancel(): Unit = {}

  //最后关闭资源
  override def close(): Unit = {
    if(rs != null){
      rs.close()
    }
    if(ps != null){
      ps.close()
    }
    if(conn != null){
      conn.close()
    }
  }
}
