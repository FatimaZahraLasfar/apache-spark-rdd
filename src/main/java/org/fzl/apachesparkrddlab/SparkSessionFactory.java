package org.fzl.apachesparkrddlab;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkSessionFactory {

    public static JavaSparkContext createContext(String appName) {

        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]");

        return new JavaSparkContext(conf);
    }
}