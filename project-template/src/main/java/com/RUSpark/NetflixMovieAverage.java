package com.RUSpark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/* any necessary Java packages here */

public class NetflixMovieAverage {
	// private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		  SparkSession spark = SparkSession
			      .builder()
			      .appName("NetflixMovieAverage")
			      .getOrCreate();
		JavaRDD<Row> rows = spark.read().csv(InputPath).javaRDD();
	JavaPairRDD<String, Integer> ones = rows.mapToPair(s -> new Tuple2<>((String)s.getString(0), 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
    List<Tuple2<String, Integer>> countsOutput = counts.collect();
	  JavaPairRDD<String, Integer> ratings = rows.mapToPair(s -> new Tuple2<>((String)s.getString(0), Integer.parseInt((String) s.get(2))));

	 JavaPairRDD<String, Integer> summedRatings = ratings.reduceByKey((i1, i2) -> i1 + i2);

	
	 List<Tuple2<String, Integer>> ratingsOutput = summedRatings.collect();
	
	    for (Tuple2<?,?> tuple : ratingsOutput) {
	    	for(Tuple2<?,?> tuple2 : countsOutput) {
	    		if(((String)tuple._1()).equals((String)tuple2._1())) {
	    			 System.out.println((String)tuple._1() + " " + String.format("%.2f", ((Integer.parseInt(tuple._2().toString())*1.0)/Integer.parseInt(tuple2._2().toString()))));
	    			break;
	    		}
	    			 
	    	}
	       }
	    spark.stop();
	
		
	}

}
