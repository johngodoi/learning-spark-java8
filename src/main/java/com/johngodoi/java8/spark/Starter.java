package com.johngodoi.java8.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class Starter
{
    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf().setAppName(Starter.class.getName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");
        JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        List<Integer> squared = nums.map(x -> x * x).collect();

        squared.forEach(System.out::println);

    }
}
