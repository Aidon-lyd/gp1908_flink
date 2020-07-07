package com.qianfeng.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 批次的java版本统计词频
*@Description
**/
public class Demo01_batchWC_Java {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dset = env.fromElements("i link flink", "flink flink flink flink is nice");
        AggregateOperator<Tuple2<String, Integer>> sumed = dset.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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
                .groupBy(0)
                .sum(1);

        //sink
        sumed.print();
    }
}
