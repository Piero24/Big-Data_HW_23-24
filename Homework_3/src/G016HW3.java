// Copyright (C) by Group 016 All Rights Reserved
//
// This file is part of the project: Homework 3
//
// Written by: Pietrobon Andrea, Friso Giovanni, Agostini Francesco
// Date: June 2024

// Command for execute the homework from terminal:
// -XX:ReservedCodeCacheSize=256m -Dspark.master="local[*]" G016HW3 1000000 0.01 0.2 0.2 8888
// -XX:ReservedCodeCacheSize=2048m -Dspark.master="local[*]" G016HW3 1000000 0.01 0.2 0.2 8888

// import java.util.*;
// import java.io.IOException;

// import scala.Tuple2;
// import scala.Tuple3;

// import org.apache.spark.SparkConf;
// import org.apache.spark.api.java.JavaRDD;
// import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.broadcast.Broadcast;

//import org.apache.arrow.flatbuf.List;
import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.*;
import java.util.List;



//! Used because in vscode the sparkContext is not recognized
//! It must be removed before the submission 
@SuppressWarnings("deprecation")
public class G016HW3 {
    // After how many items should we stop?
    // public static final int THRESHOLD = 1000000;

    /**
     * 
     *
     * @param args 
     * @throws IOException 
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: item_num, frequency, accuracy, confidence, port");
        }

        // IMPORTANT: the master must be set to "local[*]" or "local[n]" with n > 1, otherwise
        // there will be no processor running the streaming computation and your
        // code will crash with an out of memory (because the input keeps accumulating).

        SparkConf conf = new SparkConf(true)
        .setMaster("local[*]") //! remove this line if running on the cluster
        .setAppName("DistinctExample");

        // Here, with the duration you can control how large to make your batches.
        // Beware that the data generator we are using is very fast, so the suggestion
        // is to use batches of less than a second, otherwise you might exhaust the
        // JVM memory.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(10));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL:
        // The streaming spark context and our code and the tasks that are spawned all
        // work concurrently. To ensure a clean shut down we use this semaphore. The
        // main thread will first acquire the only permit available, and then it will try
        // to acquire another one right after spinning up the streaming computation.
        // The second attempt at acquiring the semaphore will make the main thread
        // wait on the call. Then, in the `foreachRDD` call, when the stopping condition
        // is met the semaphore is released, basically giving "green light" to the main
        // thread to shut down the computation.
        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        int n = Integer.parseInt(args[0]);
        System.out.println("Number of items = " + n);

        float phi = Float.parseFloat(args[1]);
        System.out.println("Frequency threshold = " + phi);

        float epsilon = Float.parseFloat(args[2]);
        System.out.println("Accuracy parameter = " + epsilon);

        float delta = Float.parseFloat(args[3]);
        System.out.println("Confidence parameter = " + delta);
        
        int portExp = Integer.parseInt(args[4]);
        System.out.println("Receiving data from port = " + portExp);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0]=0L;
        HashMap<Long, Long> histogram = new HashMap<>(); // Hash Table for the distinct elements
        HashMap<Long, Long> trueFrequent = new HashMap<>();
        int m = (int) Math.ceil(1 / phi);
        List<String> sample = new ArrayList<>();

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)

        /*Number of items processed = 1095145
        Number of distinct items = 43621
        Largest item = 4294473407

        Number of items processed = 1065816
        Number of distinct items = 42493
        Largest item = 4294473407

        Number of items processed = 1019151
        Number of distinct items = 40649
        Largest item = 4294473407*/

        // For each batch, to the following.
        // BEWARE: the `foreachRDD` method has "at least once semantics", meaning
        // that the same data might be processed multiple times in case of failure.
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
                    if (globalIndex < m) {
                        sample.add(batchItems.get(i));
                    } else {
                        double probability = (double) m / (globalIndex + 1);
                        double random = Math.random();
                        if (random < probability) {
                            int randomIndex = (int) Math.floor(Math.random() * m);
                            sample.set(randomIndex, batchItems.get(i));
                        }
                    }
                }

                if (batchSize > 0) {
                    System.out.println("Batch size at time [" + time + "] is: " + batchSize);
                }

                if (streamLength[0] >= n) {
                    stoppingSemaphore.release();
                }
            }
        });
        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");
        
        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, false);
        
        System.out.println("Streaming engine stopped");
        // COMPUTE AND PRINT FINAL STATISTICS
        System.out.println("Number of items processed = " + streamLength[0]);
        // Calculate and print frequent items
        long thresholdCount = (long) Math.ceil(phi * streamLength[0]);
        System.out.println("Threshold count = " + thresholdCount);
        System.out.println("Frequent items (count >= " + thresholdCount + "):");

        if (histogram.isEmpty()) {
            System.out.println("No items were counted.");
        }

        for (Map.Entry<Long, Long> entry : histogram.entrySet()) {
           if (entry.getValue() >= thresholdCount) {
                trueFrequent.put(entry.getKey(),entry.getValue());
            }
        }

        long first = -1;

        List<Long> keys = new ArrayList<>(trueFrequent.keySet());
        Collections.sort(keys);

        for (Long key : keys) {
            if (key > first) {
                // print or process the entry
                System.out.println(key + " -> " + trueFrequent.get(key));
            }
        }

        if (histogram.isEmpty()) {
            System.out.println("No frequent items found.");
            
        } else {
            System.out.println("N Frequent Items: " + trueFrequent.size());
        }

        sc.close();
    }
}

// 1000000 0.01 0.2 0.2 8888
/*Frequent items (count >= 10322): 1%
2784604852: 41418
434415286: 82555
1936875793: 82726
2486806931: 82704
870070186: 82343
632507218: 41194
3620094053: 82464
3837533304: 83318
978604355: 41362
3737777178: 82079
2162156987: 41194
2967394975: 82643
2788970093: 82111
195773912: 82900
Frequent items (count >= 86590): 8%
434415286: 86614
1936875793: 86694
2486806931: 86780
3837533304: 87284
2967394975: 86716
195773912: 86906*/
