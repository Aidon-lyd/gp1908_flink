package com.qianfeng.batch

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.operators.{AggregateOperator, DataSource}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode


/**
 *批次的sink
 */
object Demo04_batch_sink {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //定义kafka消费所需

    //val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val datasource: DataSource[(Int, Int)] = env.fromElements(Tuple2(200, 66), Tuple2(100, 65), Tuple2(200, 56), Tuple2(200, 666), Tuple2(100, 678))

    val res: AggregateOperator[(Int, Int)] = datasource.groupBy(0).sum(1)

    //基于文件
    res.print("print sink---")
    res.writeAsText("E:\\flinkdata\\out\\0800",WriteMode.OVERWRITE)
    res.writeAsText("hdfs://hadoop01:9000/08t/06",WriteMode.OVERWRITE)
    res.writeAsCsv("hdfs://hadoop01:9000/08t/07",WriteMode.OVERWRITE)

    //output
    res.output(new MyBatchMysqlOutputFormat)

    //触发执行
    env.execute("基础sink---")
  }
}

//自定义输出需要实现OutputFormat
class MyBatchMysqlOutputFormat extends OutputFormat[(Int,Int)]{
  //连接数据库的对象
  var conn:Connection = _
  var ps:PreparedStatement = _

  override def configure(parameters: Configuration): Unit = {
    //do nothing
  }

  //初始化mysql的连接信息
  override def open(taskNumber: Int, numTasks: Int): Unit = {
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

  //将获取到数据插入到mysql中
  //如果计算是批次，也建议批次插入；
  // 如果数量大，也建议使用druid连接池；；
  // 如果数据量较大，数据库扛不住，改用kafka、es、hbase
  override def writeRecord(element: (Int,Int)): Unit = {
    ps = conn.prepareStatement("replace into wc(dt,province,adds,possibles) values(?,?)")
    //赋值
    ps.setInt(1,element._1)
    ps.setInt(1,element._2)
    //执行插入
    ps.execute()
  }

  override def close(): Unit = {
    if(ps != null){
      ps.close()
    }
    if(conn != null){
      conn.close()
    }
  }
}
