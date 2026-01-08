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
    private static final String TRUTH_BASE_DIR = "data_generated";

    public DumpRecomputer(SparkSession spark) {
        this.spark = spark;
        this.spark.sparkContext().setLogLevel("ERROR");
    }

    public void run(String targetDate, String outputDir) {
        System.out.println("=== Recomputing Dump for " + targetDate + " ===");

        // 1. Charger l'état actuel (C'est notre point de départ, le "Futur")
        // On lui donne une date fictive très lointaine pour qu'il soit toujours considéré "après" les diffs
        Dataset<Row> current = spark.read().parquet(LATEST_PATH)
                .withColumn("day", lit("9999-12-31"))
                .withColumn("action", lit("KEEP")); // Action fictive pour dire "c'est la donnée actuelle"

        // 2. Charger l'historique inverse (seulement ce qui s'est passé APRÈS la targetDate)
        // Ce sont les "instructions de retour en arrière"
        Dataset<Row> reverseHistory = spark.read().parquet(DIFF_PATH)
                .filter(col("day").gt(targetDate)); // STRICTEMENT SUPÉRIEUR à la date cible

        // 3. Union : On met l'état actuel et l'historique dans le même Dataset
        // unionByName permet de gérer les colonnes qui pourraient ne pas être dans le même ordre
        Dataset<Row> allData = current.unionByName(reverseHistory, true);


        // --- ÉTAPE 2 : Vérification de cohérence (Preuve par 9) ---
        // Logique : (État reconstruit à Date X) + (Diffs entre X et Aujourd'hui) == (bal_latest)
        
        System.out.println("\n=== Consistency Check (Verification) ===");
        System.out.println("--- Replaying history from NOW to " + targetDate);

        Dataset<Row> snapshot = allData
                .withColumn("rn", row_number().over(
                        Window.partitionBy("cle_interop") // On groupe par ID unique
                              .orderBy(col("day").asc())  // IMPORTANT : On cherche la date la plus petite (la plus proche du passé)
                ))
                .filter(col("rn").equalTo(1)) // On ne garde que la version la plus proche de la target
                .filter(col("action").notEqual("DELETE")) // Si l'instruction inverse est "DELETE", ça veut dire que la donnée n'existait pas encore à cette date (C'était un Insert dans le futur)
                .drop("rn", "action", "day"); // On nettoie les colonnes techniques
        
        // On met le résultat en cache car on va s'en servir deux fois (Save + Verify)
        snapshot.cache();

        // 5. Sauvegarde du résultat
        snapshot.write().mode(SaveMode.Overwrite).parquet(outputDir);
        
        System.out.println("--- Dump reconstruit avec succes pour le " + targetDate);
        System.out.println("--- Sortie enregistree dans : " + outputDir);

        System.out.println("\n--- VERIFICATIONS ---");
        
        String truthFile = TRUTH_BASE_DIR + "/dump-" + targetDate + ".csv";
        
        try {
            // A. Lecture du VRAI fichier CSV de cette date
            Dataset<Row> truthRaw = spark.read()
                    .option("header", "true")
                    .option("delimiter", ";")
                    .option("inferSchema", "true")
                    .csv(truthFile);
            
            // On s'assure d'avoir les colonnes nécessaires (cle_interop, etc.)
            // On suppose que les colonnes 'commune', 'voie', 'code_postal' existent pour les stats
            
            // Calcul des stats pour le fichier RECONSTRUIT
            Stats computedStats = computeStats(snapshot);
            
            // Calcul des stats pour le fichier VRAI (ORIGINAL)
            Stats truthStats = computeStats(truthRaw);

            // Affichage du tableau comparatif
            printComparisonTable(computedStats, truthStats);

            // Vérification stricte
            if (computedStats.equals(truthStats)) {
                 System.out.println("\nSUCCES : Les statistiques correspondent parfaitement.");
            } else {
                 System.err.println("\nATTENTION : Il y a des divergences dans les statistiques.");
            }

        } catch (Exception e) {
            System.out.println("⚠️ Impossible d'effectuer la verification complete (Fichier CSV introuvable ou erreur de lecture).");
            System.out.println("   Erreur : " + e.getMessage());
        }
        
        snapshot.unpersist();        
    }

    private static class Stats {
        long count;
        long cities;
        long streets;
        long depts;

        // Constructeur simple
        public Stats(long c, long ci, long s, long d) {
            this.count = c; this.cities = ci; this.streets = s; this.depts = d;
        }

        // Pour comparer facilement
        public boolean equals(Stats other) {
            return count == other.count && cities == other.cities && streets == other.streets && depts == other.depts;
        }
    }

    private Stats computeStats(Dataset<Row> df) {
        // On s'assure que code_postal est vu comme string pour extraire les 2 premiers chars
        Dataset<Row> dfWithDept = df.withColumn("dept_ext", substring(col("commune_insee").cast("string"), 1, 2));
        
        // Aggregation en une seule passe pour être efficace
        Row result = dfWithDept.agg(
            count("*"),
            countDistinct("commune_insee"),
            countDistinct("voie_nom"),
            countDistinct(substring(col("commune_insee"), 0, 2).alias("dept"))
        ).first();

        return new Stats(result.getLong(0), result.getLong(1), result.getLong(2), result.getLong(3));
    }

    private void printComparisonTable(Stats computed, Stats truth) {
        System.out.println("\n+===================================================================+");
        System.out.println("|                 COMPARAISON AVEC LE CSV ORIGINAL                  |");
        System.out.println("+--------------------------+--------------------+-------------------+");
        System.out.println("| METRIQUE                 | CALCULE (Spark)    | REEL (CSV Orig.)  |");
        System.out.println("+--------------------------+--------------------+-------------------+");
        
        printRow("Nombre d'adresses", computed.count, truth.count);
        printRow("Nombre de villes", computed.cities, truth.cities);
        printRow("Noms de rues distincts", computed.streets, truth.streets);
        printRow("Nombre de departements", computed.depts, truth.depts);
        
        System.out.println("+===================================================================+");
    }

    private void printRow(String label, long val1, long val2) {
        System.out.printf("| %-24s | %18d | %17d |\n", label, val1, val2);
    }
}


// modifie le code dans le fichier DumpRecomputer.java pour qu'il charge au début la version de la date demandé, repasse l'historique du jour demandé jusqu'à aujourd'hui comme ce que la fonction fait actuellement, puis compare ce qui a été trouvé avec le vrai fichier qui était enregistré, sachant que je ne peux pas prendre un troisième paramètre quand j'appelle mes fonctions donc il faut que j'arrive à récupérer le fichier le plus récent