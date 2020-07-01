package com.bbd.FlinkTest.flinkSource;

import org.apache.flink.api.java.ExecutionEnvironment;
import java.util.ArrayList;
import java.util.List;

public class DatasetDataSourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollection(env);
        textFile(env);
    }

    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i=1; i<=10; i++){
            list.add(i);
        }
        env.fromCollection(list).print();
    }

    public static void textFile(ExecutionEnvironment env) throws Exception {
        String filepath = "E:\\idea_workspace\\FlinkTest\\data\\vertex_person.csv";
        env.readTextFile(filepath).print();
        System.out.println("~~~~~~~~~~~~~~~~~~~");
        filepath = "E:\\idea_workspace\\FlinkTest\\data";
        env.readTextFile(filepath).print();
    }
}
