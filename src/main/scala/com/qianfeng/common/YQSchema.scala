package com.qianfeng.common

import java.lang

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

//自定义kafka的生产序列化
class YQSchema(topic:String) extends KafkaSerializationSchema[YQ]{
  //序列化
  override def serialize(element: YQ, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    //从element取出所有需要打入到kafka中的值
    val dt: String = element.date
    val province: String = element.province
    val adds: Int = element.adds
    val possibles: Int = element.possibles
    //封装返回  --- 如果有多个分区，，需要指定key
    new ProducerRecord[Array[Byte], Array[Byte]](topic,(dt+"_"+province+"_"+adds+"_"+possibles).getBytes)
  }
}
