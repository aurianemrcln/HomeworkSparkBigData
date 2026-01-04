package fr.esilv;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.io.File;
import java.util.Arrays;

public class DailyIntegration {
    private final SparkSession spark;
    private static final String LATEST_PATH = "bal_latest";
    private static final String DIFF_PATH = "bal.db/bal_diff";

    public DailyIntegration(SparkSession spark) {
        this.spark = spark;
    }

    public void run(String date, String csvFile) {
        System.out.println("Running Integration for date: " + date);

        // 1. Read Today's CSV
        Dataset<Row> todayRaw = spark.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(csvFile);

        // --- CORRECTION ICI ---
        // On convertit les noms de colonnes (String[]) en objets (Column[])
        Column[] cols = Arrays.stream(todayRaw.columns())
                              .map(c -> col(c))
                              .toArray(Column[]::new);

        // On utilise ce tableau de Colonnes pour le hash
        Dataset<Row> today = todayRaw.withColumn("hash", sha2(concat_ws("||", cols), 256));
        // ----------------------

        // 2. Read Previous State
        Dataset<Row> previous;
        boolean isFirstRun = !new File(LATEST_PATH).exists() && !new File(LATEST_PATH + "/_SUCCESS").exists();

        if (isFirstRun) {
            System.out.println("No existing data found. Initializing full load.");
            // On first run, everything is an INSERT
            Dataset<Row> initialDiff = today.withColumn("action", lit("INSERT"))
                                            .withColumn("day", lit(date)); 
            
            initialDiff.write().mode(SaveMode.Append).partitionBy("day").parquet(DIFF_PATH);
            today.drop("hash").write().mode(SaveMode.Overwrite).parquet(LATEST_PATH);
            return;
        } else {
            previous = spark.read().parquet(LATEST_PATH)
                    // On recalcule le hash pour être sûr (en utilisant les mêmes colonnes)
                    .withColumn("hash", sha2(concat_ws("||", cols), 256));
        }

        // 3. Compute Differences
        // Assumption: 'id' column exists and is unique

        // INSERTS: ID in Today, not in Previous
        Dataset<Row> inserts = today.join(previous, today.col("id").equalTo(previous.col("id")), "left_anti")
                .withColumn("action", lit("INSERT"));

        // DELETES: ID in Previous, not in Today
        Dataset<Row> deletes = previous.join(today, previous.col("id").equalTo(today.col("id")), "left_anti")
                .withColumn("action", lit("DELETE"));

        // UPDATES: ID in both, but Hash is different
        Dataset<Row> updates = today.alias("new")
                .join(previous.alias("old"), "id")
                .filter(col("new.hash").notEqual(col("old.hash")))
                .select("new.*")
                .withColumn("action", lit("UPDATE"));

        // 4. Union all changes
        Dataset<Row> allDiffs = inserts.union(updates).union(deletes)
                .withColumn("day", lit(date)); 

        // 5. Save Diff (Partitioned by day)
        allDiffs.drop("hash")
                .write()
                .mode(SaveMode.Append)
                .partitionBy("day")
                .parquet(DIFF_PATH);

        System.out.println("Diff saved. Inserts: " + inserts.count() + ", Updates: " + updates.count() + ", Deletes: " + deletes.count());

        // 6. Update 'bal_latest' (Snapshot)
        today.drop("hash").write().mode(SaveMode.Overwrite).parquet(LATEST_PATH);
    }
}