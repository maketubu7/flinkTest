package com.bbd.FlinkTest.tools;

import scala.Int;
import scala.Tuple2;

import java.util.Random;

/**
 * @Author: maketubu
 * @Date: 2019/12/5 18:02
 */
public class randomTool {

    public static void main(String[] args) {

    }

    public static Tuple2<String, Int> getRandomEmit() {
        Random random = new Random(System.currentTimeMillis());
        String key = "类别" + (char) ('A' + random.nextInt(3));
        int value = random.nextInt(10) + 1;
        Tuple2<String, Int> result = new Tuple2<String, Int>(key, Int);
        return result;
    }
}
