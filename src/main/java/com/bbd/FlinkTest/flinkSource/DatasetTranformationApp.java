package com.bbd.FlinkTest.flinkSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.expressions.E;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
public class DatasetTranformationApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        filterFunction(env);
//        firstFunctio(env);
//        flatmapFunction(env);
        joinFunction(env);
    }


    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i=1; i<=10; i++){
            list.add(i);
        }
        env.fromCollection(list).map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer v) throws Exception {
                return v+1;
            }
        }).print();
    }

    public static void filterFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i=1; i<=10; i++){
            list.add(i);
        }
        env.fromCollection(list).map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer v) throws Exception {
                return v+1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer v) throws Exception {
                return v > 3;
            }
        }).print();
    }

    public static void firstFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer,String>> list = new ArrayList<Tuple2<Integer,String>>();
        list.add(new Tuple2<>(1,"a"));
        list.add(new Tuple2<>(1,"b"));
        list.add(new Tuple2<>(1,"c"));
        list.add(new Tuple2<>(2,"d"));
        list.add(new Tuple2<>(2,"e"));
        list.add(new Tuple2<>(2,"f"));
        list.add(new Tuple2<>(3,"g"));
        list.add(new Tuple2<>(3,"h"));
        list.add(new Tuple2<>(3,"i"));

        env.fromCollection(list).first(2).print();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~");
        env.fromCollection(list).groupBy(0).first(2).print();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~");
        env.fromCollection(list).groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();

    }

    public static void flatmapFunction(ExecutionEnvironment env) throws Exception{
        List<String> list = new ArrayList<String>();
        list.add("h,f,gs");
        list.add("h1,f,gs");
        list.add("h2,f,gs");

        env.fromCollection(list).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] splits = line.split(",");
                for (String word : splits){
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).groupBy(0).sum(1).print();

    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer,String>> list1 = new ArrayList<Tuple2<Integer,String>>();
        list1.add(new Tuple2<>(1,"a"));
        list1.add(new Tuple2<>(2,"b"));
        list1.add(new Tuple2<>(3,"c"));
        list1.add(new Tuple2<>(4,"d"));
        list1.add(new Tuple2<>(5,"e"));
        List<Tuple2<Integer,String>> list2 = new ArrayList<Tuple2<Integer,String>>();
        list2.add(new Tuple2<>(1,"a"));
        list2.add(new Tuple2<>(2,"b"));
        list2.add(new Tuple2<>(3,"c"));
        list2.add(new Tuple2<>(4,"d"));
        list2.add(new Tuple2<>(6,"f"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(list1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(list2);

        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<>(first.f0,first.f1,second.f1);
            }
        }).print();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (second == null){
                    return new Tuple3<>(first.f0,first.f1,"-");
                }
                else{
                    return new Tuple3<>(first.f0,first.f1,second.f1);
                }

            }
        }).print();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

    }
}
