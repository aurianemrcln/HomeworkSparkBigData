package fr.esilv;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil; 
import java.io.IOException;
import java.util.Arrays;

public class DailyIntegration {
    private final SparkSession spark;
    
    // Chemins des données
    private static final String LATEST_PATH = "bal_latest";
    private static final String BUFFER_PATH = "bal_processing_buffer"; // Zone de lecture sécurisée
    private static final String DIFF_PATH = "bal.db/bal_diff";

    public DailyIntegration(SparkSession spark) {
        this.spark = spark;
        this.spark.sparkContext().setLogLevel("ERROR");
    }

    public void run(String date, String csvFile) {
        System.out.println("=== Running Daily Integration (Reverse Logic) for " + date + " ===");

        try {
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            Path latestPath = new Path(LATEST_PATH);
            Path bufferPath = new Path(BUFFER_PATH);

            // ==============================================================================
            // ETAPE 0 : SAFETY BUFFER (Contournement du verrouillage fichier Windows)
            // ==============================================================================
            // On copie l'état actuel vers un tampon pour le lire tranquillement.
            // Cela permet d'écraser 'bal_latest' plus tard sans conflit de lecture/écriture.
            
            if (fs.exists(bufferPath)) {
                fs.delete(bufferPath, true); // Nettoyage préventif
            }
            
            boolean isFirstRun = !fs.exists(latestPath);
            if (!isFirstRun) {
                // Copie physique de bal_latest -> bal_processing_buffer
                FileUtil.copy(fs, latestPath, fs, bufferPath, false, spark.sparkContext().hadoopConfiguration());
            }

            // ==============================================================================
            // ETAPE 1 : Chargement des données
            // ==============================================================================
            
            // A. Lecture du CSV du jour (TODAY)
            Dataset<Row> todayRaw = spark.read()
                    .option("header", "true")
                    .option("delimiter", ";")
                    .option("inferSchema", "true")
                    .csv(csvFile);

            // Calcul du Hash sur toutes les colonnes pour détecter les modifs
            Column[] cols = Arrays.stream(todayRaw.columns())
                                .map(c -> col(c))
                                .toArray(Column[]::new);

            Dataset<Row> today = todayRaw.withColumn("hash", sha2(concat_ws("||", cols), 256));

            // B. Lecture de l'état précédent (PREVIOUS) depuis le BUFFER
            Dataset<Row> previous;

            if (isFirstRun) {
                System.out.println("--- Premier lancement detecte. Initialisation...");
                
                // Si c'est le jour 1, l'historique inverse pour revenir à "Rien" consiste à tout supprimer.
                Dataset<Row> reverseDiff = today.withColumn("action", lit("DELETE"))
                                                .withColumn("day", lit(date)); 
                
                reverseDiff.write().mode(SaveMode.Append).partitionBy("day").parquet(DIFF_PATH);
                today.drop("hash").write().mode(SaveMode.Overwrite).parquet(LATEST_PATH);

                printSummaryTable(date, today.count(), 0, 0);
                return;
            } else {
                // IMPORTANT : On lit depuis le BUFFER, pas depuis LATEST
                previous = spark.read().parquet(BUFFER_PATH)
                        .withColumn("hash", sha2(concat_ws("||", cols), 256));
            }

            // ==============================================================================
            // ETAPE 2 : Calcul des Différences (Logique Inverse / Reverse Delta)
            // ==============================================================================
            
            // 1. REVERSE DELETE : Lignes apparues aujourd'hui (Inserts).
            // Pour revenir en arrière, il faut les SUPPRIMER.
            // (Lignes présentes dans TODAY mais absentes de PREVIOUS)
            Dataset<Row> revDeletes = today.join(previous, today.col("cle_interop").equalTo(previous.col("cle_interop")), "left_anti")
                    .withColumn("action", lit("DELETE"));

            // 2. REVERSE INSERT : Lignes disparues aujourd'hui (Deletes).
            // Pour revenir en arrière, il faut les RESTAURER (Insérer).
            // (Lignes présentes dans PREVIOUS mais absentes de TODAY)
            Dataset<Row> revInserts = previous.join(today, previous.col("cle_interop").equalTo(today.col("cle_interop")), "left_anti")
                    .withColumn("action", lit("INSERT"));

            // 3. REVERSE UPDATE : Lignes modifiées.
            // Pour revenir en arrière, il faut restaurer l'ANCIENNE valeur (Celle de PREVIOUS).
            Dataset<Row> revUpdates = today.alias("new")
                    .join(previous.alias("old"), "cle_interop")
                    .filter(col("new.hash").notEqual(col("old.hash")))
                    .select("old.*") // <--- ON GARDE L'ANCIENNE VALEUR
                    .withColumn("action", lit("UPDATE"));

            // ==============================================================================
            // ETAPE 3 : Sauvegarde
            // ==============================================================================

            // A. Sauvegarde de l'historique (Diffs)
            Dataset<Row> allDiffs = revInserts.union(revUpdates).union(revDeletes)
                    .withColumn("day", lit(date)); 

            allDiffs.drop("hash")
                    .write()
                    .mode(SaveMode.Append)
                    .partitionBy("day")
                    .parquet(DIFF_PATH);

            // B. Mise à jour de l'état courant (Snapshot)
            // On écrase bal_latest avec les données d'aujourd'hui.
            // C'est sans danger car 'previous' pointe sur le dossier buffer.
            today.drop("hash").write().mode(SaveMode.Overwrite).parquet(LATEST_PATH);

            // ==============================================================================
            // ETAPE 4 : Nettoyage et Stats
            // ==============================================================================

            // On compte AVANT de supprimer le buffer (actions Spark lazy)
            long cNewToday      = revDeletes.count(); // Ce sont les "Inserts" du jour
            long cDeletedToday  = revInserts.count(); // Ce sont les "Deletes" du jour
            long cModifiedToday = revUpdates.count(); // Ce sont les "Updates" du jour
            
            // Suppression du buffer temporaire
            if (fs.exists(bufferPath)) {
                fs.delete(bufferPath, true);
            }

            printSummaryTable(date, cNewToday, cModifiedToday, cDeletedToday);

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("--- Erreur FileSystem Hadoop : " + e.getMessage());
        }
    }

    private void printSummaryTable(String date, long inserts, long updates, long deletes) {
        long total = inserts + updates + deletes;
        System.out.println("\n+================================================+");
        System.out.println("|           RECAPITULATIF JOURNALIER             |");
        System.out.println("|           Date : " + String.format("%-20s", date) + "          |");
        System.out.println("+--------------------------+---------------------+");
        // Note pour l'affichage :
        // revDeletes (Action DELETE) = C'était une NOUVELLE adresse (Insert) aujourd'hui.
        // revInserts (Action INSERT) = C'était une adresse SUPPRIMÉE (Delete) aujourd'hui.
        System.out.printf("| NOUVELLES (Inserts)      | %19d |\n", inserts);
        System.out.printf("| MODIFIEES (Updates)      | %19d |\n", updates);
        System.out.printf("| SUPPRIMEES (Deletes)     | %19d |\n", deletes);
        System.out.println("+--------------------------+---------------------+");
        System.out.printf("| TOTAL CHANGEMENTS        | %19d |\n", total);
        System.out.println("+================================================+\n");
    }
}