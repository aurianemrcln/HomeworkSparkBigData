package fr.esilv;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FileDiff {
    private final SparkSession spark;

    public FileDiff(SparkSession spark) {
        this.spark = spark;
        this.spark.sparkContext().setLogLevel("ERROR");
    }

    public void run(String path1, String path2) {
        System.out.println("=== COMPARAISON DE FICHIERS ===");
        // recupère les 10 derniers caractères des chemins pour l'affichage:
        System.out.println("Fichier A : " + path1);
        System.out.println("Fichier B : " + path2);

        try {
            // 1. Lecture des fichiers
            Dataset<Row> df1 = spark.read().parquet(path1);
            Dataset<Row> df2 = spark.read().parquet(path2);

            // 2. Calcul des Statistiques globales (pour le tableau)
            Stats s1 = computeStats(df1);
            Stats s2 = computeStats(df2);

            // 3. Affichage du Tableau Comparatif
            printComparisonTable(s1, s2);

            // ==============================================================================
            // 4. ANALYSE DÉTAILLÉE DES DIFFÉRENCES
            // ==============================================================================
            
            // On se base sur la clé primaire 'cle_interop'
            // A. Lignes supprimées (Dans A mais pas dans B)
            Dataset<Row> inAnotB = df1.join(df2, df1.col("cle_interop").equalTo(df2.col("cle_interop")), "left_anti");
            
            // B. Lignes ajoutées (Dans B mais pas dans A)
            Dataset<Row> inBnotA = df2.join(df1, df2.col("cle_interop").equalTo(df1.col("cle_interop")), "left_anti");

            // C. Lignes modifiées (Clé dans les deux, mais contenu différent)
            // On identifie les colonnes communes (hors clé) pour comparer
            List<String> columns = Arrays.asList(df1.columns());
            List<String> commonCols = columns.stream()
                    .filter(c -> !c.equals("cle_interop") && !c.equals("hash")) // On ignore la clé et le hash technique
                    .collect(Collectors.toList());

            // Construction dynamique de la condition de différence : (A.col1 != B.col1) OR (A.col2 != B.col2)...
            Column diffCondition = lit(false);
            for (String colName : commonCols) {
                // On gère les nulls avec eqNullSafe (null == null est vrai)
                // On cherche l'inverse : NOT(A == B)
                diffCondition = diffCondition.or(not(df1.col(colName).eqNullSafe(df2.col(colName))));
            }

            // Jointure interne pour récupérer les clés communes
            Dataset<Row> common = df1.alias("a").join(df2.alias("b"), "cle_interop");
            
            // On filtre ceux qui ont au moins une différence
            Dataset<Row> modified = common.filter(diffCondition);
            
            // Comptes
            long countRemoved = inAnotB.count();
            long countAdded = inBnotA.count();
            long countModified = modified.count();

            // 5. Affichage des listes
            if (countRemoved == 0 && countAdded == 0 && countModified == 0) {
                System.out.println("\nLes fichiers sont STRICTEMENT IDENTIQUES (contenu et structure).");
            } else {
                System.out.println("\nDes différences ont ete trouvees.");

                // AFFICHER LES SUPPRESSIONS
                if (countRemoved > 0) {
                    System.out.println("\n--- LIGNES SUPPRIMEES (Dans A uniquement) : " + countRemoved + " ---");
                    inAnotB.select("cle_interop", "commune_nom", "voie_nom", "numero").show(10, false);
                }

                // AFFICHER LES AJOUTS
                if (countAdded > 0) {
                    System.out.println("\n--- LIGNES AJOUTEES (Dans B uniquement) : " + countAdded + " ---");
                    inBnotA.select("cle_interop", "commune_nom", "voie_nom", "numero").show(10, false);
                }

                // AFFICHER LES MODIFICATIONS
                if (countModified > 0) {
                    System.out.println("\n--- LIGNES MODIFIEES (Meme cle, valeurs differentes) : " + countModified + " ---");
                    // Pour l'affichage, on montre la clé et les colonnes importantes des DEUX fichiers
                    // On essaye de montrer 'commune_nom' pour voir si c'est ça qui change
                    modified.select(
                        col("a.cle_interop"),
                        col("a.commune_nom").alias("old_commune"),
                        col("b.commune_nom").alias("new_commune"),
                        col("a.voie_nom").alias("old_voie"),
                        col("b.voie_nom").alias("new_voie")
                    ).show(10, false);
                }
            }

        } catch (Exception e) {
            System.err.println("--- Erreur critique lors de la comparaison : " + e.getMessage());
            e.printStackTrace();
        }
    }

    // --- Stats Helpers (Identique à avant) ---

    private static class Stats {
        long count;   // Adresses
        long cities;  // Communes
        long streets; // Voies
        long depts;   // Départements

        public Stats(long c, long ci, long s, long d) {
            this.count = c; this.cities = ci; this.streets = s; this.depts = d;
        }
    }

    private Stats computeStats(Dataset<Row> df) {
        Dataset<Row> dfClean = df.withColumn("dept_str", substring(col("commune_insee").cast("string"), 1, 2));
        
        Row result = dfClean.select(
            countDistinct("cle_interop"),
            countDistinct("commune_insee"),
            countDistinct("voie_nom"),
            countDistinct("dept_str")
        ).first();

        return new Stats(result.getLong(0), result.getLong(1), result.getLong(2), result.getLong(3));
    }

    private void printComparisonTable(Stats s1, Stats s2) {
        System.out.println("\n+=================================================================================+");
        System.out.println("|                           TABLEAU COMPARATIF                                    |");
        System.out.println("+--------------------------+-------------------------+----------------------------+");
        System.out.println("| METRIQUE                 | Fichier A (Premier)     | Fichier B (Second)         |");
        System.out.println("+--------------------------+-------------------------+----------------------------+");
        printRow("Nombre d'adresses", s1.count, s2.count);
        printRow("Nombre de villes", s1.cities, s2.cities);
        printRow("Noms de rues distincts", s1.streets, s2.streets);
        printRow("Nombre de departements", s1.depts, s2.depts);
        System.out.println("+=================================================================================+");
    }

    private void printRow(String label, long val1, long val2) {
        System.out.printf("| %-24s | %23d | %26d |\n", label, val1, val2);
    }
}