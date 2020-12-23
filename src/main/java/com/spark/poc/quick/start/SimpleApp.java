package com.spark.poc.quick.start;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.Poc;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleApp extends Poc {

    /**
     * https://spark.apache.org/docs/latest/quick-start.html
     */
    public void execute(SparkSession spark) {
        String logFile = "/spark/README.md";

        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();
        long numCs = logData.filter(s -> s.contains("c")).count();
        log.info("Lines with a: " + numAs + ", lines with b: " + numBs + ", lines with c: " + numCs);
    }
}
