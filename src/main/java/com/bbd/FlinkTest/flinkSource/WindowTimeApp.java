package com.bbd.FlinkTest.flinkSource;

import com.bbd.FlinkTest.dataSource.JavaAllDataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * Copyright@paidaxing
 * Author: paidaxing
 * Date:2020/7/4
 * Description:
 */

public class WindowTimeApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        processFunction(senv);
        senv.execute();
    }

    public static void processFunction(StreamExecutionEnvironment senv) {
        senv.addSource(new JavaAllDataSource.numbers()).map(new MapFunction<Integer, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Integer value) throws Exception {
                int key = 1;
                if (value % 2 == 0) key = 2;
                return new Tuple2<>(key,value);
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).process(new ProcessWindowFunction<Tuple2<Integer, Integer>, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) throws Exception {
                long count = 0;
                for (Tuple2<Integer, Integer> v : elements){
                    count++;
                }

                out.collect("window "+ context.window() + " count " + count);
            }
        }).print();
    }
}
