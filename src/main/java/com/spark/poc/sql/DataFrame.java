package com.spark.poc.sql;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.Poc;

public class DataFrame extends Poc {
    @Override
    protected void execute(SparkSession spark) throws AnalysisException {
        Dataset<Row> df = spark.read().json("/opt/spark-data/people.json");
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(col("name"), col("age").plus(1)).show();
        df.filter(col("age").gt(21)).show();
        df.groupBy("age").count().show();

        df.createGlobalTempView("people");
        spark.sql("select * from global_temp.people").show();
    }
}
