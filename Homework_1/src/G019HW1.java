// -XX:ReservedCodeCacheSize=256m -Dspark.master="local[*]" G019HW1 ./Homework_1/Data/TestN15-input.txt 1.0 3 9 2
// -XX:ReservedCodeCacheSize=512m -Dspark.master="local[*]" G019HW1 ./Homework_1/Data/uber-10k.csv 0.02 10 5 2
// -XX:ReservedCodeCacheSize=512m -Dspark.master="local[*]" G019HW1 ./Homework_1/Data/uber-10k.csv 0.02 10 50 2

/*
 * //: TODO:
 * * Comment the code in a proper way AS IN THE GOOGLE JAVA STYLE GUIDE (If you don't know what it is, --> RDFM <--) https://google.github.io/styleguide/javaguide.html
 * * Add the sorting method for the inputPoints RDD in the main method as explained in the pdf and in the section below (Do in that exact point for efficiency)
 * * Understand if it must print the output in a file .txt or only in the console (IDFK)
 * * Attach to each element, relative to a non-empty cell C, the values |N3(C)| and |N7(C)|
 * 
 * Why I think this is the correct file?
 * 1) The file I have sended on telegram have all the explenation highlighted in the pdf.
 * 2) The following explenation a summary of info founded on google:
 * * - If u don't cache the RDD after the creation it will be recomputed from scratch at every call
 * * - Download all non empty cells into a local data structure is MUCH better then use collection every time.
 * //! DON'T MAKE MY RUNNING TIME WORSE! I'M WATCHING YOU! 👀
 */


import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import java.util.*;

public class G019HW1 {

    /**
     * Main method to read data from a file and perform outlier detection.
     * 
     * @param args Command-line arguments: filename, D, M, K
     * @throws IOException if an I/O error occurs
     */
    public static void main(String[] args) throws IOException {

        // Check if filename is provided as command-line argument
        if (args.length < 5){
            System.out.println("Please provide filename, D, M, K, and L as command-line arguments");
            return;
        }

        // File Name
        String filename = args[0];
        // Distance Threshold 
        float D = Float.parseFloat(args[1]);
        // Number of Nearest Neighbors
        int M = Integer.parseInt(args[2]);
        // Number of Neighbors to Check
        int K = Integer.parseInt(args[3]);
        // Number of Partitions
        int L = Integer.parseInt(args[4]);

        SparkConf conf = new SparkConf(true).setAppName("G019HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> rawData = sc.textFile(filename);

        JavaRDD<Tuple2<Float, Float>> inputPoints = rawData.map(line -> {
            String[] parts = line.split(",");
            float x = Float.parseFloat(parts[0]);
            float y = Float.parseFloat(parts[1]);
            return new Tuple2<>(x, y);
        });

        /*
         *
         *  
         * * ADD SORTING METHOD IN ASCENDING ORDER HERE FOR inputPoints (Sort by x and y)
         * * Ex: [(2,1) (1,1) (0,2) (0,1)] ---> [(0,1) (0,2) (1,1) (2,1)] 
         * 
         * 
         */

        inputPoints = inputPoints.repartition(L).cache();

        clearScreen();
        System.out.println(filename + " D=" + D + " M=" + M + " K=" + K + " L=" + + L);

        long totalPoints = inputPoints.count();
        System.out.println("Number of points: " + totalPoints);

        if (totalPoints <= 200000) {

            long startTime = System.currentTimeMillis();
            List<Tuple2<Float, Float>> listOfPoints = inputPoints.collect();

            exactOutliers(listOfPoints, D, M, K);

            long endTime = System.currentTimeMillis();
            long runningTime = endTime - startTime;
            System.out.println("Running time of ExactOutliers = " + runningTime + " ms");

        }

        long startTime = System.currentTimeMillis();

        MRApproxOutliers(inputPoints, D, M, K);

        long endTime = System.currentTimeMillis();
        long runningTime = endTime - startTime;

        System.out.println("Running time of MRApproxOutliers = " + runningTime + " ms");

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
     * Performs exact outlier detection.
     * 
     * @param listOfPoints List of data points
     * @param D Distance threshold
     * @param M Minimum number of neighbors
     * @param K Number of outliers to find
     */
    public static void exactOutliers(List<Tuple2<Float, Float>> listOfPoints, float D, int M, int K) {
        List<Tuple2<Float, Float>> count = new ArrayList<>();

        for (Tuple2<Float, Float> point : listOfPoints) {
            List<Tuple2<Float, Float>> notOutliers = new ArrayList<>();

            for (Tuple2<Float, Float> point2 : listOfPoints) {

                if (notOutliers.size() > M) break;

                // Calculate the distance between point and point2
                double distanceX = Math.pow(point._1() - point2._1(), 2);
                double distanceY = Math.pow(point._2() - point2._2(), 2);
                double distance = Math.sqrt(distanceX + distanceY);

                if (distance < D) {
                    notOutliers.add(point2);
                }
            }

            if (notOutliers.size() < M + 1) {
                count.add(point);
            }
        }

        System.out.println("Number of Outliers = " + count.size());

        int i = 0;
        for (Tuple2<Float, Float> point2 : count) {
            if (i < K) {
                System.out.println("Point: (" + point2._1() + ", " + point2._2() + ")");
                i++;
                continue;
            }
            break;
        }
    }

    /**
     * Performs MR (MapReduce) approximate outlier detection.
     * 
     * @param pairsRDD RDD of Pair objects
     * @param D Distance threshold
     * @param M Minimum number of neighbors
     * @param K Number of outliers to find
     */
    
    public static void MRApproxOutliers(JavaRDD<Tuple2<Float, Float>> inputPoints, float D, int M, int K) {

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

        /// Swap key and value
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> swappedRDD = cellCountsRDD
        .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));

        // Sort by the count k
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> sortedSwappedRDD = swappedRDD
        .sortByKey(true);

        // Swap key and value back
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> sortedCellCountsRDD = sortedSwappedRDD
        .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));

        int insideR7 = 0;
        int insideR3 = 0;

        List<Tuple2<Tuple2<Integer, Integer>, Integer>> listOfCellCounts = sortedCellCountsRDD.collect();
        
        for (Tuple2<Tuple2<Integer, Integer>, Integer> cell : listOfCellCounts) {

            totalPoints += cell._2();

            if (cell._2() > M) {
                insideR7 += cell._2();
                insideR3 += cell._2();
                continue;
            }

            int x = cell._1()._1();
            int y = cell._1()._2();
            
            int minX7 = x - 3;
            int maxX7 = x + 3;
            int minY7 = y - 3;
            int maxY7 = y + 3;

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

            if (count7 > M) {
                insideR7 += cell._2();
            }

            if (count3 > M) {
                insideR3 += cell._2();
            }
        }

        System.out.println("Number of sure outliers = " + (totalPoints - insideR7));
        System.out.println("Number of uncertain points = " + (insideR7 - insideR3));

        int i = 0;
        for (Tuple2<Tuple2<Integer, Integer>, Integer> cell : listOfCellCounts) {
            if (i < K) {
                System.out.println("Cell: (" + cell._1()._1() + ", " + cell._1()._2() + ")  Size = " + cell._2());
                i++;
                continue;
            }
            break;
        }
    }
}

