package com.RUSpark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class RedditPhotoImpact {
	 private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		  SparkSession spark = SparkSession
			      .builder()
			      .appName("WordCount")
			      .getOrCreate();

	JavaRDD<Row> rows = spark.read().csv(InputPath).javaRDD();
	//JavaRDD<Tuple2<String, Integer>> impacts = rows.map(s -> new Tuple2<String, Integer>((String)s.get(0), (Integer.parseInt((String) s.get(4))+Integer.parseInt((String) s.get(5)) +Integer.parseInt((String) s.get(6)))));
	JavaPairRDD<String, Integer> impacts = rows.mapToPair(s -> new Tuple2<>((String)s.getString(0), Integer.parseInt((String) s.get(4))+Integer.parseInt((String) s.get(5)) +Integer.parseInt((String) s.get(6))));
	 JavaPairRDD<String, Integer> summedImpacts = impacts.reduceByKey((i1, i2) -> i1 + i2);
	 List<Tuple2<String, Integer>> output = summedImpacts.collect();
	    for (Tuple2<?,?> tuple : output) {
	      System.out.println(tuple._1() + " " + tuple._2());
	    }
			
	    spark.stop();
		/* Implement Here */ 
		
	}

}
