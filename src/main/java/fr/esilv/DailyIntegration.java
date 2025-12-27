package fr.esilv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import java.io.File;


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
        // French BAL files usually use semicolon ';' separator
        Dataset<Row> todayRaw = spark.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true") // safer for first run, can utilize schema for perf
                .csv(csvFile);

        // Add a Hash column to detect content changes efficiently
        // We assume 'id' is the unique primary key. We hash everything else.
        // If the CSV structure changes, this grabs all columns.
        Dataset<Row> today = todayRaw.withColumn("hash", sha2(concat_ws("||", todayRaw.columns()), 256));

        // 2. Read Previous State
        Dataset<Row> previous;
        boolean isFirstRun = !new File(LATEST_PATH).exists() && !new File(LATEST_PATH + "/_SUCCESS").exists();

        if (isFirstRun) {
            System.out.println("No existing data found. Initializing full load.");
            // On first run, everything is an INSERT
            Dataset<Row> initialDiff = today.withColumn("action", lit("INSERT"))
                                            .withColumn("day", lit(date)); // Partition column
            
            // Write Diff
            initialDiff.write().mode(SaveMode.Append).partitionBy("day").parquet(DIFF_PATH);
            
            // Write Latest
            today.drop("hash").write().mode(SaveMode.Overwrite).parquet(LATEST_PATH);
            return;
        } else {
            previous = spark.read().parquet(LATEST_PATH)
                    // Re-calculate hash for previous data to be safe (or store hash in latest)
                    .withColumn("hash", sha2(concat_ws("||", todayRaw.columns()), 256));
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
        // We take the 'new' values from today
        Dataset<Row> updates = today.alias("new")
                .join(previous.alias("old"), "id")
                .filter(col("new.hash").notEqual(col("old.hash")))
                .select("new.*")
                .withColumn("action", lit("UPDATE"));

        // 4. Union all changes
        Dataset<Row> allDiffs = inserts.union(updates).union(deletes)
                .withColumn("day", lit(date)); // Add partition column

        // 5. Save Diff (Partitioned by day)
        // We drop 'hash' before saving to save space, it's recalculated on load
        allDiffs.drop("hash")
                .write()
                .mode(SaveMode.Append)
                .partitionBy("day")
                .parquet(DIFF_PATH);

        System.out.println("Diff saved. Inserts: " + inserts.count() + ", Updates: " + updates.count() + ", Deletes: " + deletes.count());

        // 6. Update 'bal_latest' (Snapshot)
        // The new state is simply Today's CSV content.
        // We overwrite the old snapshot.
        today.drop("hash").write().mode(SaveMode.Overwrite).parquet(LATEST_PATH);
    }
}
