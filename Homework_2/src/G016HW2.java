// Copyright (C) by Group 016 All Rights Reserved
//
// This file is part of the project: Homework 2
//
// Written by: Pietrobon Andrea, Friso Giovanni, Agostini Francesco
// Date: May 2024

// Command for execute the homework from terminal:
// -XX:ReservedCodeCacheSize=256m -Dspark.master="local[*]" G016HW2 ./Homework_2/Data/TestN15-input.txt 3 5 2
// -XX:ReservedCodeCacheSize=2048m -Dspark.master="local[*]" G016HW2 ./Homework_2/Data/artificial1M_9_100.csv 10 200 4
// -XX:ReservedCodeCacheSize=2048m -Dspark.master="local[*]" G016HW2 ./Homework_2/Data/uber-large.csv 3 100 16

import java.util.*;
import java.io.IOException;

import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;


public class G016HW2 {

    // JavaSparkContext
    private static JavaSparkContext sc;

    // Broadcast the centers
    // public static Broadcast<List<Tuple2<Float, Float>>> broadcastCenters;

    /**
     * Main method that executes the MRFFT and MRApproxOutliers algorithms.
     *
     * @param args Command-line arguments: filename, M, K and L
     * @throws IOException If an I/O error occurs
     */
    public static void main(String[] args) throws IOException {

        // Check if filename is provided as command-line argument
        if (args.length < 4) {
            System.out.println("Please provide filename, M, K, and L as command-line arguments");
            return;
        }

        // File Name
        String filename = args[0];
        // Number of Nearest Neighbors
        int M = Integer.parseInt(args[1]);
        // Number of Centers
        int K = Integer.parseInt(args[2]);
        // Number of Partitions
        int L = Integer.parseInt(args[3]);

        SparkConf conf = new SparkConf(true).setAppName("G016HW2");
        conf.set("spark.locality.wait", "0s");
        sc = new JavaSparkContext(conf);
        // JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> rawData = sc.textFile(filename);

        // Load the dataset as coordinates separated by comma
        JavaRDD<Tuple2<Float, Float>> inputPoints = rawData.map(line -> {
            String[] parts = line.split(",");
            float x = Float.parseFloat(parts[0]);
            float y = Float.parseFloat(parts[1]);
            return new Tuple2<>(x, y);
        });

        inputPoints = inputPoints.repartition(L).cache();
        System.out.println(filename + " M=" + M + " K=" + K + " L=" + + L);

        long totalPoints = inputPoints.count();
        System.out.println("Number of points: " + totalPoints);

        // Apply the MRFFT algorithm
        float D = MRFFT(inputPoints, K);

        System.out.println("Radius = " + D);

        // Apply the MRApproxOutliers algorithm
        MRApproxOutliers(inputPoints, D, M);

        // Close the JavaSparkContext
        sc.close();
    }

    /**
     * Compute the squared Euclidean distance between two points.
     *
     * @param p1 A point represented as a Tuple2<Float, Float>
     * @param p2 A point represented as a Tuple2<Float, Float>
     *
     * @return The squared Euclidean distance between p1 and p2
     */
    private static double squaredEuclideanDistance(Tuple2<Float, Float> p1, Tuple2<Float, Float> p2) {
        float dx = p1._1 - p2._1;
        float dy = p1._2 - p2._2;
        return dx * dx + dy * dy;
    }

    /**
     * Implements Farthest-First Traversal algorithm, through standard sequential code.
     * NOTE: The implementation should run in O(|P| * K) time.
     *
     * @param P A set of points represented as a list of Tuple2<Float, Float>
     * @param K Number of centers
     *
     * @return An ArrayList that is a set C of K centers.
     */
    public static List<Tuple2<Float, Float>> SequentialFFT(List<Tuple2<Float, Float>> P, int K) {

        if (P.isEmpty()) {
            throw new IllegalArgumentException("Input list P is empty");
        }
        
        List<Tuple2<Float, Float>> C = new ArrayList<>(K);
        // Add the first point of P to C
        C.add(P.get(0));

        // Initialize nearDist outside the loop
        double[] nearDist = new double[P.size()];
        for (int i = 0; i < P.size(); i++) {
            nearDist[i] = squaredEuclideanDistance(C.get(0), P.get(i));
        }

        // Main loop to select K points
        for (int i = 0; i < K-1; i++) {
            double maxDist = -1;
            int max_index = -1;
            // Update nearDist, excluding points already in C
            for (int j = 0; j < P.size(); j++) {
                if(i!=0) {
                    double currentDistance = squaredEuclideanDistance(P.get(j), C.get(i));
                    
                    if (currentDistance < nearDist[j]) {
                        nearDist[j] = currentDistance;
                    }
                }
                // Find the point with maximum distance
                if (maxDist < nearDist[j]) {
                    maxDist = nearDist[j];
                    max_index = j;
                }
            }

            // Add the farthest point to C and mark its index as selected
            C.add(P.get(max_index));
        }
        return C;
    }

    /**
     * Implements MRFFT algorithm, through MapReduce.
     *
     * @param P A set of points represented as a JavaRDD<Tuple2<Float, Float>>
     * @param K Number of centers to compute
     *
     * @return The radius D of the clustering induced by the centers
     */
    public static Float MRFFT(JavaRDD<Tuple2<Float, Float>> P, int K) {

        long startTime = System.currentTimeMillis();

        // ** ROUND 1: Compute coreset
        List<Tuple2<Float, Float>> coreset = P.mapPartitions(pointsIter -> {
            List<Tuple2<Float, Float>> localCoreset = new ArrayList<>();
            List<Tuple2<Float, Float>> pointsList = new ArrayList<>();
            pointsIter.forEachRemaining(pointsList::add);
            localCoreset = SequentialFFT(pointsList, K);
            return localCoreset.iterator();
        }).cache().collect();

        long endTime = System.currentTimeMillis();
        long runningTime = endTime - startTime;

        // System.out.println("Number of points in the coreset = " + coreset.size());
        System.out.println("Running time of MRFFT Round 1 = " + runningTime + " ms");

        startTime = System.currentTimeMillis();

        // ** ROUND 2: Compute final centers
        List<Tuple2<Float, Float>> C = SequentialFFT(coreset, K);

        endTime = System.currentTimeMillis();
        runningTime = endTime - startTime;
        System.out.println("Running time of MRFFT Round 2 = " + runningTime + " ms");

        startTime = System.currentTimeMillis();

        // ** ROUND 3: Compute the radius R of the clustering induced by the centers
        // Broadcast the centers
        //broadcastCenters = sc.broadcast(C);
        Broadcast<List<Tuple2<Float, Float>>> broadcastCenters = sc.broadcast(C);

        // Compute the radius R of the clustering induced by the centers
        JavaRDD<Double> distances = P.map(point -> {
            double minDist = Float.MAX_VALUE;
            for (Tuple2<Float, Float> center : broadcastCenters.value()) {
                double dist = squaredEuclideanDistance(point, center);
                if (dist < minDist) {
                    minDist = dist;
                }
            }
            return minDist;
        }).cache();

        double maxDistance = distances.max(Comparator.naturalOrder());
        float D = (float) Math.sqrt(maxDistance);
        // float D = maxDistance.floatValue();

        endTime = System.currentTimeMillis();
        runningTime = endTime - startTime;
        System.out.println("Running time of MRFFT Round 3 = " + runningTime + " ms");

        return D;
    }

    /**
     * Performs MR (MapReduce) approximate outlier detection.
     *
     * @param inputPoints RDD of Pair objects
     * @param D Radius of the clustering induced by the centers
     * @param M Number of nearest neighbors
     */
    public static void MRApproxOutliers(JavaRDD<Tuple2<Float, Float>> inputPoints, float D, int M) {

        long totalPoints = 0;
        double lam = D / (2 * Math.sqrt(2));

        long startTime = System.currentTimeMillis();

        // ** STEP A: Transform RDD into RDD of non-empty cells with their counts
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD = inputPoints.mapToPair(pair -> {
                    Tuple2<Integer, Integer> cellId = new Tuple2<>((int) Math.floor(pair._1() / lam), (int) Math.floor(pair._2() / lam));
                    return new Tuple2<>(cellId, 1);
                })
                .filter(pair -> pair._2() > 0) // Filter out empty cells
                .reduceByKey((count1, count2) -> count1 + count2)
                .cache();

        int insideR7 = 0;
        int insideR3 = 0;

        List<Tuple2<Tuple2<Integer, Integer>, Integer>> listOfCellCounts = cellCountsRDD.collect();

        // ** STEP B: Compute the values |N3(C)| and |N7(C)| for each cell C drawn from the previous step
        List<Tuple2<Tuple2<Integer, Integer>,Tuple3<Integer, Integer, Integer>>> listOfCells = new ArrayList<>();
        for (Tuple2<Tuple2<Integer, Integer>, Integer> cell : listOfCellCounts) {

            totalPoints += cell._2();

            if (cell._2() > M) {
                insideR7 += cell._2();
                insideR3 += cell._2();
                continue;
            }

            int x = cell._1()._1();
            int y = cell._1()._2();

            // Boundaries of the R7 region
            int minX7 = x - 3;
            int maxX7 = x + 3;
            int minY7 = y - 3;
            int maxY7 = y + 3;

            // Boundaries of the R3 region
            int minX3 = x - 1;
            int maxX3 = x + 1;
            int minY3 = y - 1;
            int maxY3 = y + 1;

            int count7 = 0;
            int count3 = 0;

            for (Tuple2<Tuple2<Integer, Integer>, Integer> cell2 : listOfCellCounts) {
                int pair_x = cell2._1()._1();
                int pair_y = cell2._1()._2();

                if (pair_x >= minX7 && pair_x <= maxX7 && pair_y >= minY7 && pair_y <= maxY7) {
                    count7 += cell2._2();
                }

                if (pair_x >= minX3 && pair_x <= maxX3 && pair_y >= minY3 && pair_y <= maxY3) {
                    count3 += cell2._2();
                }
            }


            Tuple2<Tuple2<Integer, Integer>,Tuple3<Integer, Integer, Integer>> updatedPoint = new Tuple2<>(
                    cell._1(), new Tuple3<>(cell._2(),count3, count7
            ));

            listOfCells.add(updatedPoint);
            if (count7 > M) {
                insideR7 += cell._2();
            }

            if (count3 > M) {
                insideR3 += cell._2();
            }

        }

        long endTime = System.currentTimeMillis();
        long runningTime = endTime - startTime;

        System.out.println("Number of sure outliers = " + (totalPoints - insideR7));
        System.out.println("Number of uncertain points = " + (insideR7 - insideR3));

        System.out.println("Running time of MRApproxOutliers = " + runningTime + " ms");
    }

}
