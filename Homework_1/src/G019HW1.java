// -Dspark.master="local[*]" G019HW1 ./Homework_1/Data/TestN15-input.txt 1.0 3 9 2

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.rdd.RDD;

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

        System.out.println(filename + " D=" + D + " M=" + M + " K=" + K + " L=" + + L);

        // * * CHAT GPT EXPLENATION
        // Qui si crea un oggetto SparkConf per configurare l'applicazione Spark. 
        // true passato al costruttore significa che l'applicazione è in modalità master 
        // locale (utilizzando tutti i thread disponibili sulla macchina). 
        // Il metodo setAppName("G019HW1") imposta il nome dell'applicazione a "G019HW1".
        SparkConf conf = new SparkConf(true).setAppName("G019HW1");
        // * * CHAT GPT EXPLENATION
        // Viene creato un oggetto JavaSparkContext che rappresenta l'interfaccia principale 
        //per interagire con Spark. Viene passata la configurazione creata in precedenza.
        JavaSparkContext sc = new JavaSparkContext(conf);
        // * * CHAT GPT EXPLENATION
        // Questo imposta il livello di registro per Spark a "WARN", il che significa che 
        // verranno registrati solo i messaggi di avviso e di livello superiore. 
        // Ciò aiuta a mantenere puliti i log eliminando i messaggi di debug.
        sc.setLogLevel("WARN");

        // * * CHAT GPT EXPLENATION
        // Qui si legge un file di testo tramite sc.textFile(filename), dove filename è il percorso del file 
        // passato come argomento al programma. textFile legge il file di testo e lo converte in un JavaRDD 
        // di stringhe, dove ogni riga del file è un elemento del RDD.
        JavaRDD<String> rawData = sc.textFile(filename);

        // * * CHAT GPT EXPLENATION
        // Ogni riga del RDD inputRDD viene mappata a un tupla di float. 
        // La funzione map è applicata a ciascun elemento del RDD. La funzione lambda riceve ogni riga come input, 
        // la suddivide in base al carattere "," (presumibilmente il file contiene coppie di valori separati da virgola), 
        // quindi converte le due parti in numeri decimali e restituisce una nuova tupla.
        JavaRDD<Tuple2<Float, Float>> inputPoints = rawData.map(line -> {
            String[] parts = line.split(",");
            float x = Float.parseFloat(parts[0]);
            float y = Float.parseFloat(parts[1]);
            return new Tuple2<>(x, y);
        });

        // * * CHAT GPT EXPLENATION
        // repartition(L) redistribuisce i dati in L partizioni casuali. 
        // Questo è utile per il parallelismo e l'ottimizzazione delle prestazioni, specialmente se il file è 
        // grande e si desidera sfruttare al meglio le risorse del cluster.
        //
        // cache() memorizza il RDD in memoria per un accesso rapido, che può migliorare 
        // le prestazioni se il RDD viene utilizzato più volte.
        inputPoints = inputPoints.repartition(L).cache();
        
        long totalPoints = inputPoints.count();
        System.out.println("Number of points: " + totalPoints);
        
        String methdSelected;
        long startTime = System.currentTimeMillis();

        //if (totalPoints <= 200000) {
        if (false) {
            methdSelected = "ExactOutliers";
            //: NOTE: This method (inputPoints.collect()) on TestN15-input.txt with D=1.0 M=3 K=9 L=2 takes 13/15ms we need to stay under 7ms
            List<Tuple2<Float, Float>> listOfPoints = inputPoints.collect();
            exactOutliers(listOfPoints, D, M, K);

        } else {
            methdSelected = "MRApproxOutliers";

            // To print the points
            // pairsRDD.foreach(point -> System.out.println("(" + point.first + ", " + point.second + ")"));

            // MRApproxOutliers(inputPoints, D, M, K);
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> resultRDD = MRApproxOutliers(inputPoints, D, M, K);

            // To print the points
            resultRDD.collect().forEach(System.out::println);
        }

        long endTime = System.currentTimeMillis();
        long runningTime = endTime - startTime;

        System.out.println("Running time of " + methdSelected + " = " + runningTime + " ms");

        // Close the JavaSparkContext
        sc.close();
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
     * :
     * NOTE: When completed the func must become void
     * 
     * @param pairsRDD RDD of Pair objects
     * @param D Distance threshold
     * @param M Minimum number of neighbors
     * @param K Number of outliers to find
     */
    public static JavaPairRDD<Tuple2<Integer, Integer>, Integer> MRApproxOutliers(JavaRDD<Tuple2<Float, Float>> inputPoints, float D, int M, int K) {
        /**
         * * CHAT GPT EXPLENATION
         * 
         * * JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD = pairsRDD.mapToPair(pair -> { ... }):
         * 
         * Qui, ogni elemento del RDD pairsRDD viene mappato a una coppia chiave-valore dove la chiave è un oggetto Tuple2<Integer, Integer> 
         * rappresentante le coordinate della cella (ottenute dividendo le coordinate del punto per D e trasformandole in interi) e il valore è 1, 
         * indicando la presenza di un punto nella cella.
         * 
         * * mapToPair
         * 
         * Utilizzato perché si desidera trasformare ogni elemento in un elemento chiave-valore.
         * 
         * * .reduceByKey((count1, count2) -> count1 + count2):
         * 
         * Dopodiché, si esegue un'operazione di riduzione per aggregare il conteggio delle celle con lo stesso identificatore. 
         * Questo significa che si sommano i valori associati a ogni chiave, in modo da ottenere il conteggio totale delle celle.
         * 
         * La lambda count1 + count2 specifica come combinare i valori associati a una stessa chiave.
         * 
         * * .filter(pair -> pair._2() > 0):
         * 
         * Infine, si filtra il risultato per rimuovere le celle vuote. Qui, pair._2() rappresenta il valore associato 
         * a ogni chiave nella RDD risultante, e si controlla se è maggiore di zero.
         * 
         * La RDD risultante, cellCountsRDD, contiene le celle non vuote come chiavi e il loro conteggio come valore.
         * 
         */

        // ** ATTENZIONE: D NON deve mai essere 0 altrimenti ritorna come unico valore: ((2147483647,2147483647),15)
        // ** Se D impostato a zero dall'utente cosa facciamo? Accettiamo con 0 o mettiamo a 1 di default?
        double lam = D / (2 * Math.sqrt(2));
        
        // ** STEP A: Transform RDD into RDD of non-empty cells with their counts
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD = inputPoints.mapToPair(pair -> {
            Tuple2<Integer, Integer> cellId = new Tuple2<>((int) (pair._1() / lam), (int) (pair._2() / lam));
            return new Tuple2<>(cellId, 1);
        }).reduceByKey((count1, count2) -> count1 + count2)
        .filter(pair -> pair._2() > 0); // Filter out empty cells


        //? Non capisco se ste cose che ho aggiunto sono utili o meno (da rivedere)
        // // ** STEP B: Filter pairs within N3 and N7 range of the cell
        // JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD_N3 = cellCountsRDD.filter(pair -> {
        //     int x = pair._1()._1();
        //     int y = pair._1()._2();
        //     long threshold_x_low_N3 = (long) (x - lam);
        //     long threshold_x_high_N3 = (long) (x + (lam * 2));
        //     long threshold_y_low_N3 = (long) (y - lam);
        //     long threshold_y_high_N3 = (long) (y + (lam * 2));
        //     return pair._1()._1() >= threshold_x_low_N3 && pair._1()._1() <= threshold_x_high_N3 &&
        //             pair._1()._2() >= threshold_y_low_N3 && pair._1()._2() <= threshold_y_high_N3;
        // });

        // JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD_N7 = cellCountsRDD.filter(pair -> {
        //     int x = pair._1()._1();
        //     int y = pair._1()._2();
        //     long threshold_x_low_N7 = (long) (x - (3 * lam));
        //     long threshold_x_high_N7 = (long) (x + (lam * 4));
        //     long threshold_y_low_N7 = (long) (y - (3 * lam));
        //     long threshold_y_high_N7 = (long) (y + (lam * 4));
        //     return pair._1()._1() >= threshold_x_low_N7 && pair._1()._1() <= threshold_x_high_N7 &&
        //             pair._1()._2() >= threshold_y_low_N7 && pair._1()._2() <= threshold_y_high_N7;
        // });

        // To be changed with the correct values (0 is only placeholder)
        int y_p = 0;
        int x_p = 0;

        long threshold_x_low_N3 = (long) (x_p - lam);
        long threshold_x_high_N3 = (long) (x_p + (lam * 2));
        long threshold_y_low_N3 = (long) (y_p - lam);
        long threshold_y_high_N3 = (long) (y_p + (lam * 2));

        long threshold_x_low_N7 = (long) ((x_p - (3* lam)));
        long threshold_x_high_N7 = (long) (x_p + (lam * 4));
        long threshold_y_low_N7 = (long) (y_p - (3* lam));
        long threshold_y_high_N7 = (long) (y_p + (lam * 4));

        // filter all pair with x and y in the range of the cell
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD_N3 = cellCountsRDD.filter(pair -> pair._1()._1() >= threshold_x_low_N3 && pair._1()._1() <= threshold_x_high_N3 && pair._1()._2() >= threshold_y_low_N3 && pair._1()._2() <= threshold_y_high_N3);
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD_N7 = cellCountsRDD.filter(pair -> pair._1()._1() >= threshold_x_low_N7 && pair._1()._1() <= threshold_x_high_N7 && pair._1()._2() >= threshold_y_low_N7 && pair._1()._2() <= threshold_y_high_N7);

        return cellCountsRDD;
    }
}
