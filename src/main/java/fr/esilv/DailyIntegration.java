package fr.esilv;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.Arrays;

public class DailyIntegration {
    private final SparkSession spark;
    private static final String LATEST_PATH = "bal_latest";
    private static final String DIFF_PATH = "bal.db/bal_diff";

    public DailyIntegration(SparkSession spark) {
        this.spark = spark;
        // pour éviter que trop de logs s'affichent
        this.spark.sparkContext().setLogLevel("ERROR");
    }

    public void run(String date, String csvFile) {
        System.out.println("--- Running reverse integration for " + date);

        try {
            // Configuration du FileSystem Hadoop pour gérer les dossiers proprement
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            Path latestPath = new Path(LATEST_PATH);
            
            // 1. Lecture du CSV du jour
            Dataset<Row> todayRaw = spark.read()
                    .option("header", "true")
                    .option("delimiter", ";")
                    .option("inferSchema", "true")
                    .csv(csvFile);

            Column[] cols = Arrays.stream(todayRaw.columns())
                                .map(c -> col(c))
                                .toArray(Column[]::new);

            Dataset<Row> today = todayRaw.withColumn("hash", sha2(concat_ws("||", cols), 256));

            // 2. Lecture de l'état précédent
            Dataset<Row> previous;
            boolean isFirstRun = !fs.exists(latestPath);

            // System.out.println("--- AAAAAAAAAAAAAAAAAAA");

            if (isFirstRun) {
                System.out.println("--- No existing data found. Initializing full load.");
                
                Dataset<Row> reverseDiff = today.withColumn("action", lit("DELETE"))
                                                .withColumn("day", lit(date)); 
                
                reverseDiff.write().mode(SaveMode.Append).partitionBy("day").parquet(DIFF_PATH);
                today.drop("hash").write().mode(SaveMode.Overwrite).parquet(LATEST_PATH);

                printSummaryTable(date, 0, 0, today.count());
                return;
            } else {
                // System.out.println("--- BBBBBBBBBBBBBBBBBBBB");
                previous = spark.read().parquet(LATEST_PATH)
                        .withColumn("hash", sha2(concat_ws("||", cols), 256));
                // System.out.println("--- CCCCCCCCCCCCCCCCCCCC");
            }

            // 3. Calcul des différences inverses
            Dataset<Row> deletes = today.join(previous, today.col("cle_interop").equalTo(previous.col("cle_interop")), "left_anti")
                    .withColumn("action", lit("DELETE"));

            Dataset<Row> inserts = previous.join(today, previous.col("cle_interop").equalTo(today.col("cle_interop")), "left_anti")
                    .withColumn("action", lit("INSERT"));

            Dataset<Row> updates = today.alias("new")
                    .join(previous.alias("old"), "cle_interop")
                    .filter(col("new.hash").notEqual(col("old.hash")))
                    .select("old.*")
                    .withColumn("action", lit("UPDATE"));
            
            // System.out.println("--- DDDDDDDDDDDDDDDDDDDD");

            long cInserts = inserts.count();
            // System.out.println("--- 11111111111111111111");
            long cUpdates = updates.count();
            // System.out.println("--- 22222222222222222222");
            long cDeletes = deletes.count();
            // System.out.println("--- 33333333333333333333");
            
            // 4. Sauvegarde des différences
            // Ici, Spark lit 'bal_latest' (via previous) pour calculer.
            // Il est CRITIQUE de ne pas toucher à 'bal_latest' tant que cette étape n'est pas finie.
            Dataset<Row> allDiffs = inserts.union(updates).union(deletes)
                    .withColumn("day", lit(date)); 
            
            // System.out.println("--- EEEEEEEEEEEEEEEEEEEEEE");

            allDiffs.drop("hash")
                    .write()
                    .mode(SaveMode.Append)
                    .partitionBy("day")
                    .parquet(DIFF_PATH);
            
            // System.out.println("--- FFFFFFFFFFFFFFFFFFFFFF");

            // 5. Mise à jour du Snapshot (Pattern Staging)
            // Au lieu d'écraser directement, on écrit dans un dossier temporaire
            Path tempPath = new Path(LATEST_PATH + "_temp");

            // System.out.println("--- GGGGGGGGGGGGGGGGGGGG");
            
            // Écriture dans bal_latest_temp
            today.drop("hash").write().mode(SaveMode.Overwrite).parquet(tempPath.toString());

            // System.out.println("--- HHHHHHHHHHHHHHHHHHHH");

            // 6. SWAP Atomique (Échange des dossiers)
            // Maintenant que l'écriture est finie, on peut supprimer l'ancien et renommer le nouveau
            if (fs.exists(latestPath)) {
                fs.delete(latestPath, true); // true = récursif
            }
            boolean success = fs.rename(tempPath, latestPath);

            // System.out.println("--- IIIIIIIIIIIIIIIIIIIIIII");
            
            if (!success) {
                System.err.println("--- WARNING: Failed to rename temp folder to latest. Data might be in " + tempPath);
            }

            // System.out.println("--- JJJJJJJJJJJJJJJJJJJJ");

            // 7. Affichage
            printSummaryTable(date, cInserts, cUpdates, cDeletes);

            // System.out.println("--- KKKKKKKKKKKKKKKKKKKK");

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("--- FileSystem error: " + e.getMessage());
        }
    }

    private void printSummaryTable(String date, long inserts, long updates, long deletes) {
        long total = inserts + updates + deletes;
        System.out.println("\n+================================================+");
        System.out.println("|           RECAPITULATIF JOURNALIER             |");
        System.out.println("|           Date : " + String.format("%-20s", date) + "          |");
        System.out.println("+--------------------------+---------------------+");
        System.out.printf("| NOUVELLES (Inserts)      | %19d |\n", deletes);
        System.out.printf("| MODIFIEES (Updates)      | %19d |\n", updates);
        System.out.printf("| SUPPRIMEES (Deletes)     | %19d |\n", inserts);
        System.out.println("+--------------------------+---------------------+");
        System.out.printf("| TOTAL CHANGEMENTS        | %19d |\n", total);
        System.out.println("+================================================+\n");
        // on affiche l'inverse pour insert et delete car on est en reverse integration
    }
}