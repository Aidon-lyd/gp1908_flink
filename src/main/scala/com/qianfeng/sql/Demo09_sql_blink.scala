package com.qianfeng.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/*
blink的sql
1、1.10以前的版本默认sql解析引擎时alink，1.11及以后默认使用blink引擎
2、1.11以前可以使用blink，需要设置执行环境为blink
3、将创建一个对接kafka的表，然后使用ssql语句，将计算结果存储mysql
 */
object Demo09_sql_blink {
  def main(args: Array[String]): Unit = {
    //获取blink的执行环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode() //使用流式模式
      .useBlinkPlanner() //使用blink分析引擎
      .build()
    val tenv: TableEnvironment = TableEnvironment.create(settings)

    //1、创建语句DDL  --- source
    tenv.sqlUpdate(
      s"""
         |CREATE TABLE kafkaTable (
         | user_id VARCHAR,
         | item_id VARCHAR,
         | category_id VARCHAR,
         | behavior STRING,
         | ts TIMESTAMP
         |) WITH (
         | 'connector.type' = 'kafka', -- 使用kafka的连接器
         | 'connector.version' = 'universal', -- universal代表使用0.11以后，如果0.9则直接写0.9
         | 'connector.topic' = 'test',
         | 'connector.properties.0.key' = 'zookeeper.connect',
         | 'connector.properties.0.value' = 'hadoop01:2181,hadoop02:2181,hadoop03:2181',
         | 'connector.properties.1.key' = 'bootstrap.servers',
         | 'connector.properties.1.value' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
         | 'update-mode' = 'append', -- 数据更新以追加形式
         | 'connector.startup-mode' = 'latest-offset', //数据为主题中最新数据
         | 'format.type' = 'json', -- 数据格式为json
         | 'format.derive-schema' = 'true'  -- 从DDL schemal确定json解析规则
         |)
         |""".stripMargin)

    //2、编写sql查询源数据，最终将sql查询结果打入mysql中  --- 转换，，mysql的 replace into 暂时不支持
    tenv.sqlUpdate(
      s"""
         |insert into user_report
         |select
         | DATE_FORMAT(ts,'yyyy-MM-dd HH:00') dt,
         | count(*) as pv,
         | count(distinct user_id) as uv
         | from kafkaTable
         | group by DATE_FORMAT(ts,'yyyy-MM-dd HH:00')
         |""".stripMargin)

    //3、将结果下沉到mysql ---sink
    tenv.sqlUpdate(
      s"""
         |CREATE table user_report (
         | dt VARCHAR,
         | pv bigint,
         | uv bigint
         |) WITH (
         |  'connector.type' = 'jdbc',
         |  'connector.url' = 'jdbc:mysql://hadoop01:3306/test',
         |  'connector.table' = 'user_report',
         |  'connector.username' = 'root',
         |  'connector.password' = 'root',
         |  'connector.write.flush.max-rows' = '1'
         |)
         |""".stripMargin)

    //触发执行
    tenv.execute("blink-kafka2mysql")
  }
}
