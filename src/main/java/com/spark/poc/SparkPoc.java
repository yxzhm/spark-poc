package com.spark.poc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.SparkSession;
import org.reflections.Reflections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkPoc {
    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        List<Poc> pocList = getPocList();

        List<String> inputArgs = getInputArgs(args);
        SparkSession spark = SparkSession.builder().appName("Spark Poc").getOrCreate();
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

    private static List<String> getInputArgs(String[] args) {
        List<String> inputArgs = null;
        if (args != null && args.length > 0) {
            inputArgs = Arrays.asList(args[0].split(","));
        }
        return inputArgs;
    }
}
