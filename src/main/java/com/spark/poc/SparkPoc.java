package com.spark.poc;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.reflections.Reflections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkPoc {
    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        SparkSession spark = SparkSession.builder().appName("Spark Poc").getOrCreate();

        List<Poc> pocList = getPocList();
        pocList.forEach(poc -> {
            String className = poc.getClass().getName();
            log.info("----------------" + className + "----------------");
            try {
                poc.execute(spark);
            }
            catch (AnalysisException e) {
                e.printStackTrace();
            }
            log.info("----------------" + className + "----------------");
        });
    }

    private static List<Poc> getPocList() throws IllegalAccessException, InstantiationException {
        Reflections reflections = new Reflections("com.spark.poc");
        Set<Class<? extends Poc>> subClasses = reflections.getSubTypesOf(Poc.class);

        List<Poc> pocList = new ArrayList<>();
        for (Class<? extends Poc> subClass : subClasses) {
            Poc poc = subClass.newInstance();
            pocList.add(poc);
        }
        return pocList;
    }

}
