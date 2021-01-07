package com.spark.poc.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import com.spark.poc.Poc;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RddBasicExample extends Poc {
    @Override
    protected void execute(SparkSession spark) {
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // Parallelized Collections
        List<Integer> data = Arrays.asList(1, 2, 3);
        JavaRDD<Integer> distData = sc.parallelize(data);
        Integer sumOfList = distData.reduce(Integer::sum);
        log.info("List 1,2,3 sum is {}", sumOfList);
        // External DataSets
        JavaRDD<String> distFile = sc.textFile("/spark/README.md");
        Integer sumOfReadMe = distFile.map(String::length).reduce(Integer::sum);
        log.info("ReamMe string length is {}", sumOfReadMe);

        distFile.persist(StorageLevel.MEMORY_AND_DISK());
    }
}
