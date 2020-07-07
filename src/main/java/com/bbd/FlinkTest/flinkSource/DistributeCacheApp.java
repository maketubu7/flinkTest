package com.bbd.FlinkTest.flinkSource;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Copyright@paidaxing
 * Author: paidaxing
 * Date:2020/7/2
 * Description:
 */
public class DistributeCacheApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        distributeCacheFunctiom(env);
    }

    public static void distributeCacheFunctiom(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i=1; i<=10; i++){
            list.add(i);
        }

        env.registerCachedFile("E:\\idea_workspace\\FlinkTest\\output\\java\\astext_data.txt","one-ten-data");

        DataSet<Integer> data = env.fromCollection(list).map(new RichMapFunction<Integer, Integer>() {

            Integer word_top;

            @Override
            public void open(Configuration parameters) throws Exception {
                File cacheFile = getRuntimeContext().getDistributedCache().getFile("one-ten-data");
                List<String> lines = FileUtils.readLines(cacheFile);
                for (String word : lines){
                    System.out.println(word);
                }
                word_top = Integer.valueOf(lines.get(5));
            }

            @Override
            public Integer map(Integer value) throws Exception {
               return value+word_top;
            }
        });

        data.print();

    }
}
