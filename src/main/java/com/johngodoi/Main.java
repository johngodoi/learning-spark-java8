package com.johngodoi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static com.johngodoi.StringUtils.splitLinesOnWords;


public class Main {
    public static void main(String[] args) {
        sparkCoreExample(args[0]);
        sparkSQLExample();
    }

    private static void sparkSQLExample() {
        SparkSession sparkSession = SparkSession.builder().appName(Main.class.getName()).master("local[*]").enableHiveSupport().getOrCreate();
        Dataset<Row> covid = sparkSession.sqlContext().read().format("csv").option("header", "true").load("src/main/resources");
        covid.createOrReplaceTempView("covid");
        sparkSession.sql("select count(distinct `Country/Region`) from covid").show();
        sparkSession.close();
    }

    private static void sparkCoreExample(String arg) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        ctx.setLogLevel("OFF");
        JavaRDD<String> lines = ctx.textFile(arg);

        lines.cache();
        lines.take(10).forEach(IOUtils::println);
        JavaRDD<String> words = lines.flatMap(s -> splitLinesOnWords(s).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

        counts.map(count -> count._1()+ ": "+count._2()).foreach(IOUtils::println);


        JavaRDD<Integer> nums = ctx.parallelize(Arrays.asList(1, 2, 3, 4));
        List<Integer> squared = nums.map(x -> x * x).collect();

        squared.forEach(System.out::println);
        ctx.stop();
    }
}
