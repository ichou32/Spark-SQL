package org.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Application1 {
    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder()
                .appName("spark sql")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> emp =session.read().option("multiline","true").json("employies.json");
        emp.printSchema();
        emp.show();
        emp.select("id", "name", "salary").show();
        emp.filter(col("name").notEqual("ichou").and(col("salary").gt(20000))).show();
        emp.groupBy("department").count().show();
        emp.groupBy("department").avg().show();
        emp.createTempView("empView");
        session.sql("select * from empView where salary>10000").show();
        session.sql("select department, avg(salary) from empView groupBy department").show();

    }

}
