// -Dspark.master="local[*]" G019HW1 ./Homework_1/TestN15-input.txt 0 2 3 2

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

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
        if (args.length < 1) {
            System.out.println("Usage: java Main <filename>");
            return;
        }

        // Command-line arguments
        String filename = args[0];

        float D = Float.parseFloat(args[1]);
        int M = Integer.parseInt(args[2]);
        int K = Integer.parseInt(args[3]);

        // Read number of partitions
        int K1 = Integer.parseInt(args[4]);

        // Build a for that print all the args to check if they are correct
        for (int i = 0; i < args.length; i++) {
            System.out.println("Args n°:" + i + "-->" + args[i]);
        }


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        // START FIRST TASK
        //
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        ArrayList<Pair> points = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            // Read lines from the file until the end is reached
            while ((line = br.readLine()) != null) {

                String[] coordinates = line.split(",");

                if (coordinates.length == 2) {
                    double x = Double.parseDouble(coordinates[0]);
                    double y = Double.parseDouble(coordinates[1]);
                    points.add(new Pair(x, y));
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
            return;
        }

        // Perform exact outlier detection
        // exactOutliers(points, D, M, K);

        // Additional code can be added here

        
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        // START SECOND TASK
        //
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

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
        //
        // repartition(K1) redistribuisce i dati in K1 partizioni casuali. 
        // Questo è utile per il parallelismo e l'ottimizzazione delle prestazioni, specialmente se il file è 
        // grande e si desidera sfruttare al meglio le risorse del cluster.
        //
        // cache() memorizza il RDD in memoria per un accesso rapido, che può migliorare 
        // le prestazioni se il RDD viene utilizzato più volte.
        JavaRDD<String> inputRDD = sc.textFile(filename).repartition(K1).cache();

        // * * CHAT GPT EXPLENATION
        // Ogni riga del RDD inputRDD viene mappata a un oggetto Pair. 
        // La funzione map è applicata a ciascun elemento del RDD. La funzione lambda riceve ogni riga come input, 
        // la suddivide in base al carattere "," (presumibilmente il file contiene coppie di valori separati da virgola), 
        // quindi converte le due parti in numeri decimali e restituisce un nuovo oggetto Pair.
        // Si presuppone che ci sia una classe Pair definita altrove nel codice, contenente due campi di 
        // tipo double (o qualche altro tipo numerico).
        JavaRDD<Pair> pairsRDD = inputRDD.map(line -> {
            String[] parts = line.split(",");
            double first = Double.parseDouble(parts[0]);
            double second = Double.parseDouble(parts[1]);
            return new Pair(first, second);
        });

        // To print the points
        // pairsRDD.foreach(point -> System.out.println("(" + point.first + ", " + point.second + ")"));

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> resultRDD = MRApproxOutliers(pairsRDD, D, M, K);
        
        // To print the points
        resultRDD.collect().forEach(System.out::println);

        // Close the JavaSparkContext
        sc.close();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        // START THIRD TASK
        //
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&


        //
        // ADD YOUR CODE HERE FOR THE THIRD TASK
        // 
    }

    /**
     * Performs exact outlier detection.
     * 
     * @param data List of data points
     * @param D Distance threshold
     * @param M Minimum number of neighbors
     * @param K Number of outliers to find
     */
    public static void exactOutliers(ArrayList<Pair> data, float D, int M, int K) {

        ArrayList<Pair> count = new ArrayList<>();

        for (Pair point : data) {
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

    /**
     * Performs MR (MapReduce) approximate outlier detection.
     * 
     * @param pairsRDD RDD of Pair objects
     * @param D Distance threshold
     * @param M Minimum number of neighbors
     * @param K Number of outliers to find
     */
    public static JavaPairRDD<Tuple2<Integer, Integer>, Integer> MRApproxOutliers(JavaRDD<Pair> pairsRDD, float D, int M, int K) {

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

        double lam = D / (2 * Math.sqrt(2));
        
        // ** NOTA: NON sono sicuro funzioni correttamente. quando eseguo il codice ritorna un solo punto ovvero: ((2147483647,2147483647),15)
        //
        //
        // ** STEP A: Transform RDD into RDD of non-empty cells with their counts
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCountsRDD = pairsRDD.mapToPair(pair -> {
            Tuple2<Integer, Integer> cellId = new Tuple2<>((int) (pair.first / lam), (int) (pair.second / lam));
            return new Tuple2<>(cellId, 1);
        }).reduceByKey((count1, count2) -> count1 + count2)
        .filter(pair -> pair._2() > 0); // Filter out empty cells
        
        return cellCountsRDD;
        // 

        // for (Pair point : data) {
        //     int i = (int) Math.floor(point.first / lam);
        //     int j = (int) Math.floor(point.second / lam);

        //     // R3 (Cp)
        //     int maxR3X = (int) (i*2*lam);
        //     int maxR3Y = (int) (j*2*lam);

        //     int minR3X = (int) (i*(-1)*lam);
        //     int minR3Y = (int) (j*(-1)*lam);

        //     // R7 (Cp)
        //     int maxR7X = (int) (i*4*lam);
        //     int maxR7Y = (int) (j*4*lam);

        //     int minR7X = (int) (i*(-3)*lam);
        //     int minR7Y = (int) (j*(-3)*lam);

        //     for (Pair point2 : data) {
        //         if (point2.first <= maxR3X && point2.first >= minR3X && point2.second <= maxR3Y && point2.second >= minR3Y) {
        //             // Inside R3
        //             continue;

        //         } else if (point2.first <= maxR7X && point2.first >= minR7X && point2.second <= maxR7Y && point2.second >= minR7Y) {
        //             // Inside R7 but not R3
        //             continue;

        //         } else {
        //             // Out of R7 and R3
        //             continue;
        //         }
                
        //     }
        // }
    }

    /**
     * Represents a pair of double values.
     */
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
    }
}
