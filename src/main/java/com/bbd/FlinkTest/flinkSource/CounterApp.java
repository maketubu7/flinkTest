package com.bbd.FlinkTest.flinkSource;

import org.apache.calcite.linq4j.PackageMarker;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * Copyright@paidaxing
 * Author: paidaxing
 * Date:2020/7/2
 * Description: step1 定义计数器
 *              step2 注册计数器
 *              step3 获取计数器
 */
public class CounterApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        counterFunctiom(env);
    }

    public static void counterFunctiom(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i=1; i<=10; i++){
            list.add(i);
        }
        DataSet<Integer> data = env.fromCollection(list).map(new RichMapFunction<Integer, Integer>() {

            LongCounter oddCounter = new LongCounter();
            LongCounter evenCounter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("odd_num", oddCounter);
                getRuntimeContext().addAccumulator("even_num", evenCounter);
            }

            @Override
            public Integer map(Integer value) throws Exception {
                if (value % 2 == 1) {
                    oddCounter.add(1);
                } else {
                    evenCounter.add(1);
                }
                return value;
            }
        });
        String filepath = "E:\\idea_workspace\\FlinkTest\\output\\java\\out_counter";
        data.writeAsText(filepath, FileSystem.WriteMode.OVERWRITE).setParallelism(2);

        JobExecutionResult jobExecutionResult = env.execute("counterFunctiom");
        System.out.println("odd:" + jobExecutionResult.getAccumulatorResult("odd_num"));
        System.out.println("even:" + jobExecutionResult.getAccumulatorResult("even_num"));

    }
}
