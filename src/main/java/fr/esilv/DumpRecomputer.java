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
    private static final String LATEST_PATH = "bal_latest";
    // Adapte ce chemin si besoin (ex: "C:/temp/data")
    private static final String TRUTH_BASE_DIR = "data_generated"; 

    public DumpRecomputer(SparkSession spark) {
        this.spark = spark;
        this.spark.sparkContext().setLogLevel("ERROR");
    }

    public void run(String targetDate, String outputDir) {
        System.out.println("=== Recomputing Dump for " + targetDate + " ===");

        // 1. Charger l'état actuel (Le Futur)
        // On lit bal_latest
        Dataset<Row> currentRaw = spark.read().parquet(LATEST_PATH);
        
        long countCurrent = currentRaw.count();
        System.out.println("--- DEBUG: Lignes chargees depuis bal_latest (Etat final) : " + countCurrent);
        
        if (countCurrent == 0) {
            System.err.println("!!! ALERTE : bal_latest est vide ! La reconstruction sera incomplete.");
        }

        Dataset<Row> current = currentRaw
                .withColumn("day", lit("9999-12-31"))
                .withColumn("action", lit("KEEP"));

        // 2. Charger l'historique inverse (Diffs APRES la date cible)
        Dataset<Row> reverseHistory = spark.read().parquet(DIFF_PATH)
                .filter(col("day").gt(targetDate));
        
        System.out.println("--- DEBUG: Lignes d'historique (Diffs) a appliquer : " + reverseHistory.count());

        // 3. Union : On combine tout
        // On force l'union par nom pour éviter les décalages de colonnes
        Dataset<Row> allData = current.unionByName(reverseHistory, true);

        // 4. Logique Time Travel (Le plus proche du passé, trié par date ASC)
        // Exemple : Si on veut l'état au 3 Janvier.
        // On a une ligne du 4 Janvier (Diff) et une du Futur (9999).
        // Le tri ASC met le 4 Janvier en premier. C'est l'état le plus proche.
        Dataset<Row> snapshot = allData
                .withColumn("rn", row_number().over(
                        Window.partitionBy("cle_interop") 
                              .orderBy(col("day").asc()) 
                ))
                .filter(col("rn").equalTo(1)) // On garde le 1er (le plus proche de la cible)
                .filter(col("action").notEqual("DELETE")) // Si l'action la plus proche est "DELETE", la donnée n'existait pas.
                .drop("rn", "action", "day");

        // Cache pour la suite
        snapshot.cache();

        // 5. Sauvegarde
        snapshot.write().mode(SaveMode.Overwrite).parquet(outputDir);
        System.out.println("--- Dump reconstruit sauvegarde dans : " + outputDir);

        // --- VERIFICATIONS ---
        verifyWithTruth(targetDate, snapshot);
        
        snapshot.unpersist();        
    }

    private void verifyWithTruth(String targetDate, Dataset<Row> snapshot) {
        System.out.println("\n--- VERIFICATIONS ---");
        String truthFile = TRUTH_BASE_DIR + "/dump-" + targetDate + ".csv"; // ou /dump-{date}/adresses.csv selon ton générateur
        // Note: Si ton script generate_data.sh crée des dossiers dump-YYYY-MM-DD, vérifie le chemin ici.
        // Souvent c'est : TRUTH_BASE_DIR + "/dump-" + targetDate + "/adresses.csv"
        
        // Tentative de correction automatique du chemin si le fichier simple n'existe pas
        java.io.File f = new java.io.File(truthFile);
        if (!f.exists()) {
             truthFile = TRUTH_BASE_DIR + "/dump-" + targetDate + "/adresses.csv";
        }

        try {
            Dataset<Row> truthRaw = spark.read()
                    .option("header", "true")
                    .option("delimiter", ";")
                    .option("inferSchema", "true")
                    .csv(truthFile);
            
            Stats computedStats = computeStats(snapshot);
            Stats truthStats = computeStats(truthRaw);

            printComparisonTable(computedStats, truthStats);

            if (computedStats.equals(truthStats)) {
                 System.out.println("\nSUCCES : Les statistiques correspondent parfaitement.");
            } else {
                 System.err.println("\nATTENTION : Divergences detectees !");
            }

        } catch (Exception e) {
            System.out.println("⚠️ Verification impossible (Fichier introuvable : " + truthFile + ")");
        }
    }

    // --- Classes Stats ---

    private static class Stats {
        long count, cities, streets, depts;
        public Stats(long c, long ci, long s, long d) {
            this.count=c; this.cities=ci; this.streets=s; this.depts=d;
        }
        public boolean equals(Stats o) {
            return count==o.count && cities==o.cities && streets==o.streets && depts==o.depts;
        }
    }

    private Stats computeStats(Dataset<Row> df) {
        // Correction du substring (1, 2) pour les 2 premiers caractères
        // Cast en string pour être sûr (commune_insee est souvent un int)
        Dataset<Row> dfClean = df.withColumn("dept_str", substring(col("commune_insee").cast("string"), 1, 2));

        Row r = dfClean.select(
            count("*"),
            countDistinct("commune_insee"),
            countDistinct("voie_nom"),
            countDistinct("dept_str")
        ).first();

        return new Stats(r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3));
    }

    private void printComparisonTable(Stats c, Stats t) {
        System.out.println("\n+===================================================================+");
        System.out.println("|                 COMPARAISON AVEC LE CSV ORIGINAL                  |");
        System.out.println("+--------------------------+--------------------+-------------------+");
        System.out.println("| METRIQUE                 | CALCULE (Spark)    | REEL (CSV Orig.)  |");
        System.out.println("+--------------------------+--------------------+-------------------+");
        printRow("Nombre d'adresses", c.count, t.count);
        printRow("Nombre de villes", c.cities, t.cities);
        printRow("Noms de rues distincts", c.streets, t.streets);
        printRow("Nombre de departements", c.depts, t.depts);
        System.out.println("+===================================================================+");
    }

    private void printRow(String l, long v1, long v2) {
        System.out.printf("| %-24s | %18d | %17d |\n", l, v1, v2);
    }
}