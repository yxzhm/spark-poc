package com.spark.poc.quick.start;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.Poc;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

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

        JavaRDD<String> data = spark.sparkContext().textFile(logFile,2).toJavaRDD();
        JavaPairRDD<String, Integer> counts = data.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);
        counts.saveAsTextFile("/opt/spark-data/word_count");
    }
}
