package com.spark.poc.rdd;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.Poc;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PassingFunction extends Poc {
    @Override
    protected void execute(SparkSession spark) {
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<String> lines = sc.textFile("/spark/README.md");
        JavaRDD<Integer> lineLengths = lines.map((Function<String, Integer>) String::length);

        Integer totalLength = lineLengths.reduce((Function2<Integer, Integer, Integer>) Integer::sum);

        log.info("total length is {}", totalLength);

    }
}
