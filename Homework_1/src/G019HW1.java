// /usr/bin/env /Library/Internet\ Plug-Ins/JavaAppletPlugin.plugin/Contents/Home/bin/java -cp /var/folders/1l/0mcd8pss79z5vls6p23myym40000gn/T/cp_5pc67ijo7dn0s3rgheh2f9r6g.jar G019HW1 ./Homework_1/TestN15-input.txt

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class G019HW1 {
    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("Usage: java Main <filename>");
            return;
        }

        String filename = args[0];
        ArrayList<Pair> points = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            // Read lines from the file until the end is reached
            while ((line = br.readLine()) != null) {

                String[] coordinates = line.split(",");

                if (coordinates.length == 2) {
                    double x = Double.parseDouble(coordinates[0]);
                    double y = Double.parseDouble(coordinates[1]);
                    double z = 0;
                    points.add(new Pair(x, y));
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
            return;
        }

        // Print the points (just for verification)
        // for (Pair point : points) {
        //     System.out.println(point.toString());
        // }
        float D = Float.parseFloat(args[1]);
        int M = Integer.parseInt(args[2]);
        int K = Integer.parseInt(args[3]);

        myMethods.exactOutliers(points, D, M, K);

        //
        // Add the code here ...
        //
    }

    static class Pair {

        private double first;
        private double second;

        public Pair(double first, double second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toString() {
            return "(" + first + ", " + second + ")";
        }
    
        // Getters and setters (optional)
    
        // Override toString() for readability (optional)
    }

    static class myMethods {

        public static void exactOutliers(ArrayList<Pair> data, float D, int M, int K) {

            ArrayList<Pair> count = new ArrayList<>();

            for (Pair point : data) {
                // System.out.println(point.toString());

                ArrayList<Pair> notOutliers = new ArrayList<>();

                for (Pair point2 : data) {
                    // Calculate the distance between point and point2
                    double distanceX = Math.pow(point.first - point2.first, 2);
                    double distanceY = Math.pow(point.second - point2.second, 2);
                    double distance = Math.sqrt(distanceX + distanceY);

                    if (distance < D) {
                        notOutliers.add(point2);
                    }
                }

                if (notOutliers.size() < M + 1) {
                    System.out.println("Outlier: " + point.toString());
                    count.add(point);

                } else {
                    System.out.println("Not Outlier");
                }

                 for (Pair point2 : notOutliers) {
                     System.out.println(point2.toString());
                 }
            }

            int i = 0;
            for (Pair point2 : count) {
                if (i < K) {
                    System.out.println(point2.toString());
                    i++;
                    continue;
                }
                break;
            }

        }

        public void mrApproxOutliers() {

            //
            // Add the code here ...
            //

        }
    }
}
