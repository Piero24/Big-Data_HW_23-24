// Copyright (C) by Group 016 All Rights Reserved
//
// This file is part of the project: Homework 3
//
// Written by: Pietrobon Andrea, Friso Giovanni, Agostini Francesco
// Date: June 2024

// Command for execute the homework from terminal:
// -XX:ReservedCodeCacheSize=256m -Dspark.master="local[*]" G016HW3 1000000 0.07 0.03 0.1 8888
// -XX:ReservedCodeCacheSize=2048m -Dspark.master="local[*]" G016HW3 1000000 0.07 0.03 0.1 8888

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.*;

//! Used because in vscode the sparkContext is not recognized
//! It must be removed before the submission
// @SuppressWarnings("deprecation")

public class G016HW3 {

    /**
     * Main method that executes the distinct elements algorithm.
     * The algorithm computes the distinct elements in a stream of data.
     * The algorithm uses the Reservoir Sampling and Sticky Sampling techniques.
     *
     * @param args item_num, frequency, accuracy, confidence, port
     * @throws IOException if the input parameters are not correct
     * @throws InterruptedException if the semaphore is interrupted
     * @throws IllegalArgumentException if the input parameters are not correct
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: item_num, frequency, accuracy, confidence, port");
        }

        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") //! remove this line if running on the cluster
                .setAppName("DistinctExample");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(10));
        sc.sparkContext().setLogLevel("ERROR");

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        int n = Integer.parseInt(args[0]);
        float phi = Float.parseFloat(args[1]);
        float epsilon = Float.parseFloat(args[2]);
        float delta = Float.parseFloat(args[3]);
        int portExp = Integer.parseInt(args[4]);
        System.out.println("INPUT PROPERTIES\nn = " + n + " phi = " + phi + " epsilon = " + epsilon + " delta = " + delta + " port = " + portExp);

        // Stream length (an array to be passed by reference)
        long[] streamLength = new long[1];
        streamLength[0]=0L;

        // Hash Table for the distinct elements
        HashMap<Long, Long> histogram = new HashMap<>();
        HashMap<Long, Long> trueFrequent = new HashMap<>();
        int m = (int) Math.ceil(1 / phi);
        double r = Math.log(1/(delta*phi))/epsilon;
        List<Long> sample = new ArrayList<>();
        HashMap<Long, Long> stickySample = new HashMap<>();


        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)

                // For each batch, do the following.
                .foreachRDD((batch, time) -> {
                    if (streamLength[0] < n) {
                        long batchSize = batch.count();
                        long remaining = n - streamLength[0];

                        if (batchSize > remaining) {
                            batch = batch.zipWithIndex().filter(t -> t._2 < remaining).map(t -> t._1);
                            batchSize = remaining;
                        }
                        streamLength[0] += batchSize;

                        // Update histogram
                        JavaPairRDD<String, Long> batchItemCounts = batch.mapToPair(item -> new Tuple2<>(item, 1L))
                                .reduceByKey(Long::sum);

                        // Collect the counts to the driver
                        Map<String, Long> batchCounts = batchItemCounts.collectAsMap();
                        synchronized (histogram) {
                            for (Map.Entry<String, Long> entry : batchCounts.entrySet()) {
                                Long key = Long.parseLong(entry.getKey()); // Parse the key from String to Long
                                histogram.put(key, histogram.getOrDefault(key, 0L) + entry.getValue());
                            }
                        }

                        // Reservoir sampling
                        List<String> batchItems = batch.collect(); // Collect batch items to a list
                        for (int i = 0; i < batchSize; i++) {
                            long globalIndex = streamLength[0] - batchSize + i; // Index in the entire stream
                            long batchLong = Long.parseLong(batchItems.get(i));

                            if (globalIndex < m) {
                                sample.add(batchLong);

                            } else {
                                double probability = (double) m / (globalIndex + 1);
                                double random = Math.random();

                                if (random < probability) {
                                    int randomIndex = (int) Math.floor(Math.random() * m);
                                    sample.set(randomIndex, batchLong);
                                }
                            }

                            // Sticky sampling
                            if (stickySample.containsKey(batchLong)) {
                                stickySample.put(batchLong, stickySample.get(batchLong) + 1);

                            } else {
                                double stickyProbability = r / n;
                                double stickyRandom = Math.random();

                                if (stickyRandom < stickyProbability) {
                                    stickySample.put(batchLong, 1L);
                                }
                            }
                        }

                        if (streamLength[0] >= n) {
                            stoppingSemaphore.release();
                        }
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        sc.start();
        stoppingSemaphore.acquire();
        sc.stop(false, false);

        // COMPUTE AND PRINT FINAL STATISTICS
        System.out.println("EXACT ALGORITHM");
        System.out.println("Number of items in the data structure = " + histogram.size());
        
        // Calculate and print frequent items
        long thresholdCount = (long) Math.ceil(phi * streamLength[0]);

        for (Map.Entry<Long, Long> entry : histogram.entrySet()) {
            if (entry.getValue() >= thresholdCount) {
                trueFrequent.put(entry.getKey(),entry.getValue());
            }
        }

        System.out.println("Number of true frequent items = " + trueFrequent.size());

        long first = -1;

        List<Long> keys = new ArrayList<>(trueFrequent.keySet());
        Collections.sort(keys);
        System.out.println("True frequent items:");

        for (Long key : keys) {
            if (key > first) {
                System.out.println(key);
            }
        }
        System.out.println("RESERVOIR SAMPLING");
        System.out.println("Size m of the sample = " + m);
        //System.out.println("N Frequent Items Estimated by Reservoir Sampling before removal: " + sample.size());

        Set<Long> set = new HashSet<>(sample);
        sample.clear();
        sample.addAll(set);

        Collections.sort(sample);
        System.out.println("Number of estimated frequent items = " + sample.size());
        System.out.println("Estimated frequent items: ");

        for (Long item : sample) {
            boolean found = false;

            for (long key : trueFrequent.keySet())
                if (item == key)
                    found = true;

            if (!found)
                System.out.println(item + " -");
            else
                System.out.println(item + " +");
        }

        System.out.println("STICKY SAMPLING");
        // At the end, return all items in stickySample with frequency >= (phi - epsilon) * n
        List<Long> frequentItems = new ArrayList<>();

        for (Map.Entry<Long, Long> entry : stickySample.entrySet()) {
            if (entry.getValue() >= (phi - epsilon) * n) {
                frequentItems.add(entry.getKey());
            }
        }

        //System.out.println("r: " + r);
        System.out.println("Number of items in the Hash Table = " + stickySample.size());
        System.out.println("Number of estimated frequent items = " + frequentItems.size());
        Collections.sort(frequentItems);
        
        System.out.println("Estimated frequent items: ");

        for (Long item : frequentItems) {
            boolean found = false;

            for (long key : trueFrequent.keySet()) {
                if (item == key) {
                    found = true;
                }
            }
            
            if (!found)
                System.out.println(item + " -");
            else
                System.out.println(item + " +");
        }

        sc.close();
    }
}