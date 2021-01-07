package com.spark.poc.sql;

import com.spark.poc.Poc;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class JoinExample extends Poc {
    @Override
    protected void execute(SparkSession spark) throws AnalysisException {
        JavaPairRDD<String, String> rddNameAddr = spark.sparkContext()
                .textFile("/opt/spark-data/name_addr.txt", 2)
                .toJavaRDD()
                .mapToPair(x -> pairFun().call(x))
                .cache();
        JavaPairRDD<String, String> rddNamePhone = spark.sparkContext()
                .textFile("/opt/spark-data/name_phone.txt", 2)
                .toJavaRDD()
                .mapToPair(x -> pairFun().call(x))
                .cache();

        JavaRDD<String> rddNameAddrPhone = rddNameAddr.join(rddNamePhone).map(new Function<Tuple2<String, Tuple2<String, String>>, String>() {
            @Override
            public String call(Tuple2<String, Tuple2<String, String>> input) {
                return input._1 + " " + input._2._1 + " " + input._2._2;
            }
        });
        rddNameAddr.saveAsTextFile("/opt/spark-data/userinfo");
    }

    private PairFunction<String, String, String> pairFun() {
        return new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] s1 = s.split(" ");
                return new Tuple2<>(s1[0], s1[1]);

            }
        };
    }
}
