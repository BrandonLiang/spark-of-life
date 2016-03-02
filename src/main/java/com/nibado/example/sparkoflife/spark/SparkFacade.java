package com.nibado.example.sparkoflife.spark;

import com.nibado.example.sparkoflife.salesman.City;
import com.nibado.example.sparkoflife.salesman.Route;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static com.nibado.example.sparkoflife.salesman.Route.formatDistance;

public final class SparkFacade {
    private static final Logger LOG = LoggerFactory.getLogger(SparkFacade.class);

    private SparkConf config;
    private JavaSparkContext sc;

    public SparkFacade(String appName, String master) {
        LOG.info("Starting {} on {}", appName, master);
        config = new SparkConf().setAppName(appName).setMaster(master);
    }

    /**
     * Initializes the spark context
     */
    public void init() {
        sc = new JavaSparkContext(config);
    }

    /**
     * Stops the spark context.
     */
    public void stop() {
        sc.stop();
    }

    /**
     * Solves the travelling salesman problem using a genetic algorithm that converges on an approximate
     * 'shortest' solution.
     *
     * It does a fork-solve-recombine (parallize-map-reduce) sequence for 'iterations' times. The work
     * is split into a number of Work packages depending on the amount of available workers.
     *
     * If there is no improvement in the best distance between iterations the solver will terminate.
     *
     * @param cities the list of cities to create a route for.
     * @param iterations max for-solve-recombine iterations that should be done
     * @param maxDuration max duration for a single iteration
     *
     * @return the shortest route.
     */
    public Route solveFor(List<City> cities, int iterations, int maxDuration) {
        LOG.info("Solving for {} cities in {} iterations ({} ms max iteration duration)", cities.size(), iterations, maxDuration);
        LOG.info("{}", cities.stream().map(City::getName).sorted().collect(Collectors.toList()));
        LOG.info("Parallelism: {}", sc.defaultParallelism());

        Work work = Work.forCities(cities, maxDuration);
        double start = work.shortest().getDistance();

        for(int i = 0; i < iterations;i++) {
            double iterStart = work.shortest().getDistance();
            JavaRDD<Work> dataSet = sc.parallelize(work.fork(sc.defaultParallelism()));

            work = dataSet.map(Work::solve).reduce(Work::combine);

            LOG.info("Iteration {} result: {}", i, formatDistance(work.shortest().getDistance()));
            if(iterStart == work.shortest().getDistance()) {
                LOG.info("No change; terminating.");
                break;
            }
        }

        double result = work.shortest().getDistance();
        String percent = String.format(Locale.ROOT, "%.2f", result / start * 100.0);

        LOG.info("Final result: {} -> {} ({}%)", formatDistance(start), formatDistance(result), percent);

        return work.shortest();
    }

    /**
     * SparkPi example used for testing.
     *
     * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/JavaSparkPi.java
     *
     * @return an approximation of Pi.
     */
    public double pi() {
        LOG.info("Solving Pi");
        int slices = 2;
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
        LOG.info("Pi is roughly {}", pi);

        return pi;
    }
}
