package com.spark.poc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkPoc {
    public static void main(String[] args) {
        String logFile = "/spark/README.md";
        SparkSession spark = SparkSession.builder().appName("Spark Poc").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s->s.contains("a")).count();
        long numBs = logData.filter(s->s.contains("b")).count();

        log.info("Lines with a: "+numAs+", lines with b: "+numBs);

        //spark.read().format("json").option("multiline",true).json("");
    }
}
