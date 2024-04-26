// Copyright (C) by Group 019 All Rights Reserved
//
// This file is part of the project: Homework 2
//
// Written by: Pietrobon Andrea, Friso Giovanni, Agostini Francesco
// Date: Apr 2024

// Command for execute the homowork from terminal:
// -XX:ReservedCodeCacheSize=256m -Dspark.master="local[*]" G019HW1 ./Homework_2/Data/TestN15-input.txt 1.0 3 9 2
// -XX:ReservedCodeCacheSize=512m -Dspark.master="local[*]" G019HW1 ./Homework_2/Data/uber-10k.csv 0.02 10 5 2
// -XX:ReservedCodeCacheSize=512m -Dspark.master="local[*]" G019HW1 ./Homework_2/Data/uber-10k.csv 0.02 10 50 2

import java.util.*;
import java.io.IOException;

import scala.Tuple2;
import scala.Tuple3;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class G016HW2 {

    /**
     * 
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        // Check if filename is provided as command-line argument
        if (args.length < 5) {
            System.out.println("Please provide filename, D, M, K, and L as command-line arguments");
            return;
        }

        // File Name
        String filename = args[0];
        // Distance Threshold
        float D = Float.parseFloat(args[1]);
        // Number of Nearest Neighbors
        int M = Integer.parseInt(args[2]);
        // Number of Clusters
        int K = Integer.parseInt(args[3]);
        // Number of Partitions
        int L = Integer.parseInt(args[4]);

        SparkConf conf = new SparkConf(true).setAppName("G019HW2");
        JavaSparkContext sc = new JavaSparkContext(conf);
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

        clearScreen();
        System.out.println(filename +  " D=" + D + " M=" + M + " K=" + K + " L=" + + L);

        long totalPoints = inputPoints.count();
        System.out.println("Number of points: " + totalPoints);

        System.out.println("Centers:");
        //
        List<Tuple2<Float, Float>> C;
        List<Tuple2<Float, Float>> P = inputPoints.collect();
        C = sequentialFFT(P,K);
        /*for(Tuple2<Float,Float> c: C)
        {
            System.out.println("(" + c._1() + ", " + c._2() + ")");
        }*/
        // D = MRFFT(inputPoints, K);

        //
        MRApproxOutliers(inputPoints, D, M);

        // Close the JavaSparkContext
        sc.close();
    }

    /**
     * Clears the console screen.
     *
     */
    public static void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }

    /**
     *
     *
     * @param inputPoints
     * @param D
     * @param M
     */
    public static void MRApproxOutliers(JavaRDD<Tuple2<Float, Float>> inputPoints, float D, int M) {

        long totalPoints = 0;
        double lam = D / (2 * Math.sqrt(2));

        // ** STEP A: Transform RDD into RDD of non-empty cells with their counts
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD = inputPoints.mapToPair(pair -> {
                    Tuple2<Integer, Integer> cellId = new Tuple2<>((int) Math.floor(pair._1() / lam), (int) Math.floor(pair._2() / lam));
                    return new Tuple2<>(cellId, 1);
                }).cache()
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
                cell._1(), new Tuple3<>(cell._2(),count3,count7
                ));

            listOfCells.add(updatedPoint);
            if (count7 > M) {
                insideR7 += cell._2();
            }

            if (count3 > M) {
                insideR3 += cell._2();
            }

        }

        System.out.println("Number of sure outliers = " + (totalPoints - insideR7));
        System.out.println("Number of uncertain points = " + (insideR7 - insideR3));
    }

    /**
     * The implementation should run in O(|P| * K) time.
     *
     * @param P
     * @param K
     * 
     * @return An ArrayList that is a set C of K centers.
     */
    public static List<Tuple2<Float, Float>> sequentialFFT(List<Tuple2<Float, Float>> P, int K) {

        List<Tuple2<Float, Float>> C = new ArrayList<>();
        // Make a copy of P because it is not possible to change a list resulting from an RDD
        List<Tuple2<Float, Float>> copyOfP = new ArrayList<>(P);
        // Randomly select the first point
        Tuple2<Float, Float> center = P.get(0);
        C.add(center);
        copyOfP.remove(center);

        while (C.size() < K)
        {
            double maxDistance = 0;
            // Calculate distances from each point to the current center
            for (Tuple2<Float, Float> p : copyOfP)
            {

                double dx = center._1() - p._1();
                double dy = center._2() - p._2();
                double distance = Math.sqrt(dx * dx + dy * dy);
                if (distance > maxDistance)
                {
                    maxDistance = distance;
                    center = p;
                    //System.out.println("Distanza: " + distance);
                    //System.out.println("Punto: " + p._1()+ ", " + p._2());
                }
            }

            // Add the farthest point to the centers
            C.add(center);
            //break;
            copyOfP.remove(center);
        }

        return C;
    }

    /**
     *! Pp must be changed in P (as asked by the prof "otherwise it will be strongly penalized") but return an error
     *
     * @param P
     * @param K
     * 
     * @return
     */
    public static Float MRFFT(JavaRDD<Tuple2<Float, Float>> P, int K) {

        Float D = 0.0f;
        List<Tuple2<Float, Float>> Pp = P.collect();
        List<Tuple2<Float, Float>> C = sequentialFFT(Pp, K);
        
        //
        //* Your code here
        //
        
        return D;
    }




}
