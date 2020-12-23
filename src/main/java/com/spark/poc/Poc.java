package com.spark.poc;

import org.apache.spark.sql.SparkSession;

public abstract class Poc {
     protected abstract void execute(SparkSession spark);
}
