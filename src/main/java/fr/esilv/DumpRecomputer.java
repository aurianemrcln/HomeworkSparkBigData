package fr.esilv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;


public class DumpRecomputer {
    private final SparkSession spark;
    private static final String DIFF_PATH = "bal.db/bal_diff";

    public DumpRecomputer(SparkSession spark) {
        this.spark = spark;
    }

    public void run(String targetDate, String outputDir) {
        System.out.println("Recomputing Dump for date: " + targetDate);

        // 1. Load all diffs up to the target date
        Dataset<Row> history = spark.read().parquet(DIFF_PATH)
                .filter(col("day").leq(targetDate));

        // 2. Reduce logic: Get the LATEST action for every ID
        // We partition by ID and order by date descending to find the most recent change
        Dataset<Row> snapshot = history
                .withColumn("rn", row_number().over(Window.partitionBy("id").orderBy(col("day").desc())))
                .filter(col("rn").equalTo(1)) // Keep only the latest entry
                .filter(col("action").notEqual("DELETE")) // Remove if the last action was DELETE
                .drop("rn", "action", "day"); // Clean up metadata columns

        // 3. Save result
        snapshot.write().mode(SaveMode.Overwrite).parquet(outputDir);
        System.out.println("Dump saved to: " + outputDir);
    }
}
