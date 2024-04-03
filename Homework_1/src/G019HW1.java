// -XX:ReservedCodeCacheSize=256m -Dspark.master="local[*]" G019HW1 ./Homework_1/Data/TestN15-input.txt 1.0 3 9 2
// -XX:ReservedCodeCacheSize=512m -Dspark.master="local[*]" G019HW1 ./Homework_1/Data/uber-10k.csv 0.02 10 5 2

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class G019HW1 {

    /**
     * Main method to read data from a file and perform outlier detection.
     * 
     * @param args Command-line arguments: filename, D, M, K
     * @throws IOException if an I/O error occurs
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

        inputPoints = inputPoints.repartition(L).cache();

        clearScreen();
        System.out.println(filename + " D=" + D + " M=" + M + " K=" + K + " L=" + + L);

        long totalPoints = inputPoints.count();
        System.out.println("Number of points: " + totalPoints);

        if (totalPoints <= 200000) {

            long startTime = System.currentTimeMillis();

            //: NOTE: This method (inputPoints.collect()) on TestN15-input.txt with D=1.0 M=3 K=9 L=2 takes 13/15ms we need to stay under 7ms
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

        long totalPoints = inputPoints.count();
        double lam = D / (2 * Math.sqrt(2));
        
        // ** STEP A: Transform RDD into RDD of non-empty cells with their counts
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD = inputPoints.mapToPair(pair -> {
            Tuple2<Integer, Integer> cellId = new Tuple2<>((int) Math.floor(pair._1() / lam), (int) Math.floor(pair._2() / lam));
            return new Tuple2<>(cellId, 1);
        }).reduceByKey((count1, count2) -> count1 + count2)
        .filter(pair -> pair._2() > 0); // Filter out empty cells

        int insideR7 = 0;
        int insideR3 = 0;
        
        // ** STEP B: Check each cell for outliers
        for (Tuple2<Tuple2<Integer, Integer>, Integer> cell2 : cellCountsRDD.collect()) {
            int x = cell2._1()._1();
            int y = cell2._1()._2();
            
            int minX7 = x - 3;
            int maxX7 = x + 3;
            int minY7 = y - 3;
            int maxY7 = y + 3;

            int minX3 = x - 1;
            int maxX3 = x + 1;
            int minY3 = y - 1;
            int maxY3 = y + 1;
            
            // Filter neighboring cells
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD_N7 = cellCountsRDD.filter(pair -> {
                int pair_x = pair._1()._1();
                int pair_y = pair._1()._2();
                
                return pair_x >= minX7 && pair_x <= maxX7 && pair_y >= minY7 && pair_y <= maxY7;
            });

            JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD_N3 = cellCountsRDD.filter(pair -> {
                int pair_x = pair._1()._1();
                int pair_y = pair._1()._2();
                
                return pair_x >= minX3 && pair_x <= maxX3 && pair_y >= minY3 && pair_y <= maxY3;
            });
            
            // Transform the RDD to a pair RDD where keys are the same and values represent counts
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> countPairRDD7 = cellCountsRDD_N7.mapToPair(cell -> new Tuple2<>(cell._1(), cell._2()));
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> countPairRDD3 = cellCountsRDD_N3.mapToPair(cell -> new Tuple2<>(cell._1(), cell._2()));

            // Aggregate counts for each key
            int count7 = countPairRDD7.values().reduce(Integer::sum);
            int count3 = countPairRDD3.values().reduce(Integer::sum);

            if (count7 > M) {
                insideR7 += cell2._2();
            }

            if (count3 > M) {
                insideR3 += cell2._2();
            }
        }

        System.out.println("Number of sure outliers = " + (totalPoints - insideR7));
        System.out.println("Number of uncertain points = " + (insideR7 - insideR3));

        int i = 0;
        for (Tuple2<Tuple2<Integer, Integer>, Integer> cell2 : cellCountsRDD.collect()) {
            if (i < K) {
                System.out.println("Cell: (" + cell2._1()._1() + ", " + cell2._1()._2() + ")  Size = " + cell2._2());
                i++;
                continue;
            }
            break;
        }
    }
    
}
