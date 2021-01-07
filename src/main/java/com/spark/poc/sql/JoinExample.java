package com.spark.poc.sql;

import com.spark.poc.Poc;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class JoinExample extends Poc {
    @Override
    protected void execute(SparkSession spark) throws AnalysisException {
        JavaPairRDD<String, String> rddNameAddr = spark.sparkContext()
                .textFile("/opt/spark-data/name_addr.txt", 2)
                .toJavaRDD()
                .mapToPair(x -> {
                    String[] s = x.split(" ");
                    return new Tuple2<>(s[0],s[1]);
                })
                .cache();
        JavaPairRDD<String, String> rddNamePhone = spark.sparkContext()
                .textFile("/opt/spark-data/name_phone.txt", 2)
                .toJavaRDD()
                .mapToPair(x -> {
                    String[] s = x.split(" ");
                    return new Tuple2<>(s[0],s[1]);
                })
                .cache();

        JavaRDD<String> rddNameAddrPhone = rddNameAddr.join(rddNamePhone).map(new Function<Tuple2<String, Tuple2<String, String>>, String>() {
            @Override
            public String call(Tuple2<String, Tuple2<String, String>> input) {
                return input._1 + " " + input._2._1 + " " + input._2._2;
            }
        });
//        rddNameAddrPhone.saveAsTextFile("/opt/spark-data/userinfo");
        rddNameAddrPhone.collect().forEach(System.out::println);
    }

}
