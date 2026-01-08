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
        System.out.println("--- Running Integration for date: " + date);

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

            if (isFirstRun) {
                System.out.println("--- No existing data found. Initializing full load.");
                long count = today.count();
                
                Dataset<Row> initialDiff = today.withColumn("action", lit("INSERT"))
                                                .withColumn("day", lit(date)); 
                
                initialDiff.write().mode(SaveMode.Append).partitionBy("day").parquet(DIFF_PATH);
                today.drop("hash").write().mode(SaveMode.Overwrite).parquet(LATEST_PATH);

                printSummaryTable(date, count, 0, 0);
                return;
            } else {
                previous = spark.read().parquet(LATEST_PATH)
                        .withColumn("hash", sha2(concat_ws("||", cols), 256));
            }

            // 3. Calcul des Différences (Lecture de previous)
            Dataset<Row> inserts = today.join(previous, today.col("cle_interop").equalTo(previous.col("cle_interop")), "left_anti")
                    .withColumn("action", lit("INSERT"));

            Dataset<Row> deletes = previous.join(today, previous.col("cle_interop").equalTo(today.col("cle_interop")), "left_anti")
                    .withColumn("action", lit("DELETE"));

            Dataset<Row> updates = today.alias("new")
                    .join(previous.alias("old"), today.col("cle_interop").equalTo(previous.col("cle_interop")))
                    .filter(col("new.hash").notEqual(col("old.hash")))
                    .select("new.*")
                    .withColumn("action", lit("UPDATE"));

            // 4. Sauvegarde des différences
            // Ici, Spark lit 'bal_latest' (via previous) pour calculer.
            // Il est CRITIQUE de ne pas toucher à 'bal_latest' tant que cette étape n'est pas finie.
            Dataset<Row> allDiffs = inserts.union(updates).union(deletes)
                    .withColumn("day", lit(date)); 

            allDiffs.drop("hash")
                    .write()
                    .mode(SaveMode.Append)
                    .partitionBy("day")
                    .parquet(DIFF_PATH);

            // Pour l'affichage final
            long cInserts = inserts.count();
            long cUpdates = updates.count();
            long cDeletes = deletes.count();

            // 5. Mise à jour du Snapshot (Pattern Staging)
            // Au lieu d'écraser directement, on écrit dans un dossier temporaire
            Path tempPath = new Path(LATEST_PATH + "_temp");
            
            // Écriture dans bal_latest_temp
            today.drop("hash").write().mode(SaveMode.Overwrite).parquet(tempPath.toString());

            // 6. SWAP Atomique (Échange des dossiers)
            // Maintenant que l'écriture est finie, on peut supprimer l'ancien et renommer le nouveau
            if (fs.exists(latestPath)) {
                fs.delete(latestPath, true); // true = récursif
            }
            boolean success = fs.rename(tempPath, latestPath);
            
            if (!success) {
                System.err.println("--- WARNING: Failed to rename temp folder to latest. Data might be in " + tempPath);
            }

            // 7. Affichage
            printSummaryTable(date, cInserts, cUpdates, cDeletes);

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
        System.out.println("| TYPE DE MODIFICATION     | NOMBRE D'ADRESSES   |");
        System.out.println("+--------------------------+---------------------+");
        System.out.printf("| NOUVELLES (Inserts)      | %19d |\n", inserts);
        System.out.printf("| MODIFIEES (Updates)      | %19d |\n", updates);
        System.out.printf("| SUPPRIMEES (Deletes)     | %19d |\n", deletes);
        System.out.println("+--------------------------+---------------------+");
        System.out.printf("| TOTAL CHANGEMENTS        | %19d |\n", total);
        System.out.println("+================================================+\n");
    }
}