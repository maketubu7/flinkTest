package com.bbd.FlinkTest.dataSource;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.ArrayList;
import java.util.Random;

/**
 * Copyright@paidaxing
 * Author: paidaxing
 * Date:2020/7/2
 * Description: 四种自定义source
 */
public class JavaAllDataSource {
    public static class words extends RichSourceFunction<String> {
        private volatile boolean isRunning = true;
        String prefix = "rand";

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                ArrayList<String> datas = new ArrayList<>();
                Random random = new Random();
                datas.add(prefix + random.nextInt(5));
                datas.add(prefix + random.nextInt(5));
                datas.add(prefix + random.nextInt(5));
                Thread.sleep(1000);
                ctx.collect(String.join("-", datas));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class parallwords extends RichParallelSourceFunction<String> {
        private volatile boolean isRunning = true;
        String prefix = "rand";

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                ArrayList<String> datas = new ArrayList<>();
                Random random = new Random();
                datas.add(prefix + random.nextInt(5));
                datas.add(prefix + random.nextInt(5));
                datas.add(prefix + random.nextInt(5));
                Thread.sleep(1000);
                ctx.collect(String.join("-", datas));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class student_str extends RichSourceFunction<String> {
        private volatile boolean isRunning = true;
        String[] xms = "李四,张三,王五,赵六,麻子".split(",");
        Random random = new Random();
        Integer id = 0;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                Thread.sleep(1000);
                String xm = id.toString() + "," + xms[random.nextInt(5)];
                ctx.collect(xm);
                id += 1;
                if (id > 100) isRunning = false;
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class numbers extends RichSourceFunction<Integer> {
        private volatile boolean isRunning = true;
        Random random = new Random();
        Integer count = 0;
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (isRunning) {
                Thread.sleep(1000);
                Integer randdint = random.nextInt(99);
                System.out.println("source "+ randdint);
                ctx.collect(randdint);
                count += 1;
                if (count > 100) isRunning = false;
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
