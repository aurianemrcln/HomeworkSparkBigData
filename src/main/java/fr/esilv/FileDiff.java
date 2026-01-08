package fr.esilv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class FileDiff {
    private final SparkSession spark;

    public FileDiff(SparkSession spark) {
        this.spark = spark;

        this.spark.sparkContext().setLogLevel("ERROR");
    }

    public void run(String path1, String path2) {
        System.out.println("--- Comparing " + path1 + " vs " + path2);

        Dataset<Row> df1 = spark.read().parquet(path1);
        Dataset<Row> df2 = spark.read().parquet(path2);

        // Rows in 1 but not 2
        Dataset<Row> diff1 = df1.except(df2);
        
        // Rows in 2 but not 1
        Dataset<Row> diff2 = df2.except(df1);

        long count1 = diff1.count();
        long count2 = diff2.count();

        if (count1 == 0 && count2 == 0) {
            System.out.println("--- SUCCESS: Files are identical.");
        } else {
            System.err.println("--- FAILURE: Files differ.");
            System.err.println("--- Rows in " + path1 + " but not in " + path2 + ": " + count1);
            System.err.println("--- Rows in " + path2 + " but not in " + path1 + ": " + count2);
            diff1.show(5);
            diff2.show(5);
            System.exit(1); // Exit with error code for the shell script test
        }
    }
}
