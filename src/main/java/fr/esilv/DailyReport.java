package fr.esilv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class DailyReport {
    private final SparkSession spark;
    private static final String LATEST_PATH = "bal_latest";

    public DailyReport(SparkSession spark) {
        this.spark = spark;
    }

    public void run() {
        System.out.println("Generating Daily Report...");
        try {
            Dataset<Row> latest = spark.read().parquet(LATEST_PATH);

            System.out.println("--- Total Addresses ---");
            System.out.println(latest.count());

            System.out.println("--- Addresses per Department ---");
            // Assuming 'code_postal' exists. Department is usually first 2 chars.
            latest.withColumn("dept", substring(col("code_postal"), 0, 2))
                  .groupBy("dept")
                  .count()
                  .orderBy("dept")
                  .show(50); // Show top 50 departments

        } catch (Exception e) {
            System.err.println("Could not read bal_latest. Has the integration run yet?");
        }
    }
}
