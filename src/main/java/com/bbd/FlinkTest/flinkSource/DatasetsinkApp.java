package com.bbd.FlinkTest.flinkSource;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class DatasetsinkApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        astextFunction(env);
        env.execute();
    }
    public static void astextFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i=1; i<=10; i++){
            list.add(i);
        }
        String filepath1 = "E:\\idea_workspace\\FlinkTest\\output\\java\\astext_data.txt";
        String filepath2 = "E:\\idea_workspace\\FlinkTest\\output\\java\\astext_data";
        env.fromCollection(list).writeAsText(filepath1, FileSystem.WriteMode.OVERWRITE);
        env.fromCollection(list).writeAsText(filepath2, FileSystem.WriteMode.OVERWRITE).setParallelism(2);
    }
}
