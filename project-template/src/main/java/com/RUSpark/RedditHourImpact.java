package com.RUSpark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/* any necessary Java packages here */

public class RedditHourImpact {
	// private static final Pattern SPACE = Pattern.compile(" ");
	public static String getHour(String unixtime) {
		Long ut = Long.parseLong(unixtime);
		Long hour=((ut%(24*60*60))/3600)-4;
		if(hour<0) {
			hour = 24+hour;
		}
		return hour+"";
	}
	public static Tuple2<String, Integer> getPair(Row r){
		String timestamp = (String)r.getString(1);
		int interactions = (Integer.parseInt((String) r.get(4))+Integer.parseInt((String) r.get(5)) +Integer.parseInt((String) r.get(6)));
		String hour = getHour(timestamp);
		return new Tuple2<>(hour, interactions);
	}
	public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		String InputPath = args[0];
		  SparkSession spark = SparkSession
			      .builder()
			      .appName("RedditHourImpact")
			      .getOrCreate();
	JavaRDD<Row> rows = spark.read().csv(InputPath).javaRDD();
		JavaPairRDD<String, Integer> impacts = rows.mapToPair(s -> getPair(s));
	 JavaPairRDD<String, Integer> summedImpacts = impacts.reduceByKey((i1, i2) -> i1 + i2);
	 List<Tuple2<String, Integer>> output = summedImpacts.collect();
	/*
	 int max = 0;
	 String maxid="";
	    for (Tuple2<?,?> tuple : output) {
	    	if(max<Integer.parseInt(tuple._2().toString())) {
	    		max = Integer.parseInt(tuple._2().toString());
	    		maxid=(String) tuple._1();
	    	}
	     // System.out.println(tuple._1() + " " + tuple._2());
	    }
	    
	   System.out.println("best hour: "+maxid+" with "+max);
	   */
	 	for(int i=0; i<24; i++) {
	    for (Tuple2<?,?> tuple : output) {
	    	if(tuple._1().equals(i+""))
	      System.out.println(tuple._1() + " " + tuple._2());
	    }
	 	}
	 	
	    spark.stop();
		
		
	}

}
