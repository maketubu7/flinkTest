package com.bbd.FlinkTest.flinkSource;

import com.bbd.FlinkTest.dataSource.JavaAllDataSource.words;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Copyright@paidaxing
 * Author: paidaxing
 * Date:2020/7/2
 * Description: 自定义datastreamSource
 */
public class DatastreamTranformationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        customSourceFunctiom(env);
        env.execute();
    }

    public static void customSourceFunctiom(StreamExecutionEnvironment env) throws Exception {
        env.addSource(new words()).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> ctx) throws Exception {
                String[] splits = value.split("-");
                for (String word : splits){
                    if (word != null & !word.equals("")){
                        ctx.collect(word);
                    }
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value,1);
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();
    }
}
