package com.qianfeng.stream

import java.util
import java.util.Properties

import com.qianfeng.common.{YQ, YQSchema}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.yarn.cli.FlinkYarnSessionCli
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


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
*1、算出每天、省份的adds、possible
*2、将如上计算结果打入到es中
 */
object Demo18_stream_ESSink {
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
        YQ(date_province(0), date_province(1), y._2._1, y._2._2)
      })

    //将其添加到sink中
    val hosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("hadoop01",9200,"http"))
    hosts.add(new HttpHost("hadoop02",9200,"http"))
    hosts.add(new HttpHost("hadoop03",9200,"http"))

    val esBuilder: ElasticsearchSink.Builder[YQ] = new ElasticsearchSink.Builder[YQ](hosts, new MyESSinkFunction)
    //设置批次操作的缓存条数(文档数量) --- 如果需要实时，就需要设置成1
    esBuilder.setBulkFlushMaxActions(1)
    val esSink: ElasticsearchSink[YQ] = esBuilder.build()
    //添加sink
    res.addSink(esSink)
    //触发执行
    env.execute("es sink connector---")
  }
}

//自定义ES的sink，，需要实现ElasticsearchSinkFunction；；泛型输入的数据类型
class MyESSinkFunction extends ElasticsearchSinkFunction[YQ]{
  //将每一行输入的数据打入到ES中
  override def process(element: YQ, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
    //es需要的数据是json或者map
    val map: util.HashMap[String, String] = new java.util.HashMap[String, String]()
    map.put("dt",element.date)
    map.put("province",element.province)
    map.put("adds",element.adds+"")
    map.put("possibles",element.possibles+"")

    //构建请求
    val indexRequest: IndexRequest = Requests.indexRequest()
      .index("yq_report_dayly")
      .`type`("info")
      .id(element.date + "_" + element.province)
      .source(map)

    //将indexRequest提交
    indexer.add(indexRequest)
  }
}