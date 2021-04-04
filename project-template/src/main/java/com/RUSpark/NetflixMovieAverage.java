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
		  System.out.println(1);
	JavaRDD<Row> rows = spark.read().csv(InputPath).javaRDD();
	JavaPairRDD<String, Integer> ones = rows.mapToPair(s -> new Tuple2<>((String)s.getString(0), 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
    List<Tuple2<String, Integer>> countsOutput = counts.collect();
	  System.out.println(2);
	//JavaRDD<Tuple2<String, Integer>> impacts = rows.map(s -> new Tuple2<String, Integer>((String)s.get(0), (Integer.parseInt((String) s.get(4))+Integer.parseInt((String) s.get(5)) +Integer.parseInt((String) s.get(6)))));
	//JavaPairRDD<String, Integer> impacts = rows.mapToPair(s -> new Tuple2<>(s.getString(0), (s.getInt(4)+s.getInt(5)+s.getInt(6))));
	  JavaPairRDD<String, Integer> ratings = rows.mapToPair(s -> new Tuple2<>((String)s.getString(0), Integer.parseInt((String) s.get(2))));

	  System.out.println(3);
	 JavaPairRDD<String, Integer> summedRatings = ratings.reduceByKey((i1, i2) -> i1 + i2);

	  System.out.println(4);
	
	 List<Tuple2<String, Integer>> ratingsOutput = summedRatings.collect();
	System.out.println(5);
	    for (Tuple2<?,?> tuple : ratingsOutput) {
	    	for(Tuple2<?,?> tuple2 : countsOutput) {
	    		if(((String)tuple._1()).equals((String)tuple2._1())) {
	    			System.out.println("helloo");
	    		// System.out.println((String)tuple._1() + " " + String.format("%.2f", (Integer.parseInt((String)tuple._2()))*1.0/(Integer.parseInt((String)tuple2._2()))));
	    			 System.out.println((String)tuple._1() + " " + tuple._2()+" "+tuple2._2());
	 	    		System.out.println("hi");
	    			break;
	    		}
	    			 
	    	}
	       }
	   System.out.println(6);
	    spark.stop();
		/* Implement Here */ 
		
	}

}
