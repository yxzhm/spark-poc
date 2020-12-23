package com.spark.poc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.SparkSession;

import com.spark.poc.quick.start.SimpleApp;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkPoc {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Spark Poc").getOrCreate();
        List<Poc> pocList = new ArrayList<>();
        pocList.add(new SimpleApp());

        List<String> inputArgs = null;
        if (args != null && args.length > 0) {
            inputArgs = Arrays.asList(args[0].split(","));
        }

        for (int i = 0; i < pocList.size(); i++) {
            if (inputArgs != null && !inputArgs.contains(String.valueOf(i))) {
                continue;
            }

            Poc poc = pocList.get(i);
            String className = poc.getClass().getName();
            log.info("----------------" + className + "----------------");
            poc.execute(spark);
            log.info("----------------" + className + "----------------");

        }
    }
}
