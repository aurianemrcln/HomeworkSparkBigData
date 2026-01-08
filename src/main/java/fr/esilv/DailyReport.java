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

        this.spark.sparkContext().setLogLevel("ERROR");
    }

    public void run() {
        System.out.println("--- Generating Daily Report...");
        try {
            Dataset<Row> latest = spark.read().parquet(LATEST_PATH);
            latest.cache();

            // System.out.println("--- Total Addresses ---");
            // System.out.println(latest.count());

            long totalAddress = latest.count();
            // On compte les éléments distincts (villes et rues)
            long nbVilles = latest.select("commune_insee").distinct().count();
            long nbRues = latest.select("voie_nom").distinct().count();
            long nbDepartements = latest.select(substring(col("commune_insee"), 0, 2).alias("dept")).distinct().count();

            // 2. Affichage du tableau global
            System.out.println("\n+================================================+");
            System.out.println("|                  STATISTIQUES                  |");
            System.out.println("+--------------------------+---------------------+");
            System.out.printf("| Nombre d'adresses        | %19d |\n", totalAddress);
            System.out.printf("| Nombre de villes         | %19d |\n", nbVilles);
            System.out.printf("| Noms de rues distinctes  | %19d |\n", nbRues);
            System.out.printf("| Nombre departements      | %19d |\n", nbDepartements);
            System.out.println("+================================================+\n");
            

            // System.out.println("--- Addresses per Department ---");
            // // Assuming 'code_postal' exists. Department is usually first 2 chars.
            // latest.withColumn("dept", substring(col("commune_insee"), 0, 2))
            //       .groupBy("dept")
            //       .count()
            //       .orderBy("dept")
            //       .show(50); // Show top 50 departments

        } catch (Exception e) {
            System.err.println("Could not read bal_latest. Has the integration run yet?");
        }
    }
}
