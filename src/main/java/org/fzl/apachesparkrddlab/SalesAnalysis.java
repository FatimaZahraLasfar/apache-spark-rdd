package org.fzl.apachesparkrddlab;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.LocalDate;

public class SalesAnalysis {

    // Partie 1 : Calculer le total des ventes par ville
    public static void salesByCity(JavaSparkContext sc, String filePath) {

        JavaRDD<String> lines = sc.textFile(filePath);

        JavaPairRDD<String, Double> sales = lines.mapToPair(line -> {

            String[] fields = line.trim().split("\\s+");

            String city = fields[1];
            double price = Double.parseDouble(fields[fields.length - 1]);

            return new Tuple2<>(city, price);
        });

        JavaPairRDD<String, Double> totals = sales
                .reduceByKey(Double::sum)
                .sortByKey();

        System.out.println("\n===== TOTAL SALES BY CITY =====");

        totals.collect().forEach(result ->
                System.out.println(
                        "City: " + result._1()
                                + " | Total: " + result._2()
                )
        );
    }

    // Partie 2 : Calculer le total des ventes par ville et par année
    public static void salesByCityAndYear(
            JavaSparkContext sc, String filePath) {

        JavaRDD<String> lines = sc.textFile(filePath);

        JavaPairRDD<String, Double> sales = lines
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .filter(line -> line.split("\\s+").length >= 4)
                .mapToPair(line -> {
                    String[] fields = line.split("\\s+");
                    String city = fields[1];
                    int year = LocalDate.parse(fields[0]).getYear();
                    double price = Double.parseDouble(fields[fields.length - 1]);
                    String key = city + " - " + year;
                    return new Tuple2<>(key, price);
                });

        JavaPairRDD<String, Double> totals = sales
                .reduceByKey(Double::sum)
                .sortByKey();

        System.out.println("\n===== TOTAL SALES BY CITY AND YEAR =====");

        totals.collect().forEach(r ->
                System.out.println("City/Year: " + r._1()
                        + " | Total: " + r._2()));
    }
}