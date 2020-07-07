package com.bbd.FlinkTest.flinkSource;

import com.bbd.FlinkTest.dataSource.JavaAllDataSource.student_str;
import com.bbd.FlinkTest.utils.DButiles;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class DatasetsinkApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
//        astextFunction(env);
//        env.execute();
        mysqlSinkFunction(senv);
        senv.execute();
    }
    public static void astextFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i=1; i<=10; i++){
            list.add(i);
        }
        String filepath1 = "E:\\idea_workspace\\FlinkTest\\output\\java\\astext_data.txt";
        String filepath2 = "E:\\idea_workspace\\FlinkTest\\output\\java\\astext_data";
        //一个分区输出为文件
        env.fromCollection(list).writeAsText(filepath1, FileSystem.WriteMode.OVERWRITE);
        //多个分区输出为文件夹
        env.fromCollection(list).writeAsText(filepath2, FileSystem.WriteMode.OVERWRITE).setParallelism(2);
    }

    public static void mysqlSinkFunction(StreamExecutionEnvironment env) throws Exception {
        env.addSource(new student_str()).addSink(new RichSinkFunction<String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("~~~~~parallerlize core~~~~");
            }

            @Override
            public void close() throws Exception {
                DButiles.returnConnection();
            }

            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.printf("~~~~invoke %s~~~~",value);
                System.out.println();
                String[] splits = value.split(",");
                Tuple2<Integer,String> stduent = new Tuple2<>(Integer.valueOf(splits[0]),splits[1]);
                DButiles.insertinsertIntoStudetn(stduent);
            }
        });
    }



}
