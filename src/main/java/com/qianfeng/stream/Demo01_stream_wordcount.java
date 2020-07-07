package com.qianfeng.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date fliink的java版本词频统计
*@Description
**/
public class Demo01_stream_wordcount {
    public static void main(String[] args) throws Exception {
        //1、执行环境 --- 包
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、源 source
        DataStreamSource<String> dstream = env.socketTextStream("hadoop01", 6666);
        //3、转换 transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = dstream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //拆分
                String[] fields = value.split(" ");
                //循环写出
                for (String field : fields) {
                    //输出
                    out.collect(new Tuple2<>(field, 1));
                }
            }
        })
                .keyBy(0)
                .sum(1);

        //4、持久化  --- sink
        res.print("java-wc-");

        //5、触发执行
        env.execute("java wordcout");
    }
}
