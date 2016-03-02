package com.nibado.example.sparkoflife;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * SparkPi example used for testing.
 *
 * Based on https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/JavaSparkPi.java
 */
public class PiExample {
    public static void main(String... argv) {
        System.out.println("Solving Pi");

        JavaSparkContext sc = new JavaSparkContext("local[4]", "PiSample");

        int slices = sc.defaultParallelism();
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = sc.parallelize(l, slices);

        int count = dataSet.map((Function<Integer, Integer>) integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y < 1) ? 1 : 0;
        }).reduce((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);

        double pi = 4.0 * (double)count / (double)n;
        System.out.println("Pi is roughly " + pi);
        sc.stop();
    }
}
