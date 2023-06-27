package org.example;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.year;

public class Indestrie {
    public static void main(String[] args) throws AnalysisException {
        SparkSession session = SparkSession.builder()
                .appName("Incident processing app")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> incedentData = session.read().option("multiline","true")
                .option("header","true")
                .csv("indestrie.csv");
//        incedentData.printSchema();
//        incedentData.show();
//        Q1: afficher les incident par service
//        Dataset<Row> incentByService = incedentData.groupBy(col("Service")).count();
//        incentByService.show();
        incedentData.createTempView("view");
        Dataset<Row> incidentByYear = incedentData.withColumn("year", year(col("Date")));
//        incidentByYear
//                .groupBy(col("Service")).count().orderBy("year").show();
        Dataset<Row> result = session.sql("select  count(*) as totalIncidents, YEAR(Date) as year" +
                " from view " +
                " GROUP BY YEAR(Date)" +
                " ORDER BY totalIncidents DESC" +
                " LIMIT 2");
        result.show();

    }
}
