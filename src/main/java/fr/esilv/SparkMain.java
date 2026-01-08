package fr.esilv;

import org.apache.spark.sql.SparkSession;
import java.util.Arrays;


public class SparkMain {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: SparkMain <JOB_TYPE> [args...]");
            System.exit(1);
        }

        String jobType = args[0];
        String[] jobArgs = Arrays.copyOfRange(args, 1, args.length);

        SparkSession spark = SparkSession.builder()
                .appName("BAL Project - " + jobType)
                .getOrCreate();
        
        spark.sparkContext().setLogLevel("ERROR");

        try {
            switch (jobType) {
                case "INTEGRATION":
                    if (jobArgs.length < 2) throw new IllegalArgumentException("INTEGRATION requires date and csvFile");
                    new DailyIntegration(spark).run(jobArgs[0], jobArgs[1]);
                    break;
                case "REPORT":
                    new DailyReport(spark).run();
                    break;
                case "RECOMPUTE":
                    if (jobArgs.length < 2) throw new IllegalArgumentException("RECOMPUTE requires date and outputDir");
                    new DumpRecomputer(spark).run(jobArgs[0], jobArgs[1]);
                    break;
                case "DIFF":
                    if (jobArgs.length < 2) throw new IllegalArgumentException("DIFF requires dir1 and dir2");
                    new FileDiff(spark).run(jobArgs[0], jobArgs[1]);
                    break;
                default:
                    System.err.println("Unknown job type: " + jobType);
                    System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }    
}
