package com.RUSpark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/* any necessary Java packages here */

public class NetflixGraphGenerate {
	
	public static List<String> generateStringList(String s) {
		List<String> a = new ArrayList<String>();
		String[] s2 = s.split(" ");
		for(String str: s2) {
			a.add(str);
		}
		return a;
	}
	private static List<String> generatePairs(List<String> s) {
		List<String> a = new ArrayList<String>();
		for(String str:s) {
			for(String str2:s) {
				if(Integer.parseInt(str)<Integer.parseInt(str2)) {
					a.add("("+str+", "+str2+")");
				}
			}
		}
		return a;
	}
	//private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }
    SparkSession spark = SparkSession
		      .builder()
		      .appName("NetflixGraphGenerate")
		      .getOrCreate();
		String InputPath = args[0];
		JavaRDD<Row> rows = spark.read().csv(InputPath).javaRDD();
		JavaPairRDD<Tuple2<String, Integer>, String> mrpair = rows.mapToPair(s->new Tuple2<>(new Tuple2<>((String)s.getString(0), Integer.parseInt((String)s.get(2))), (String)s.getString(1)));
		JavaPairRDD<Tuple2<String, Integer>, String>  summedMRPairs = mrpair.reduceByKey((i1, i2) -> i1 +" "+ i2);
		JavaPairRDD<Tuple2<String, Integer>, List<String>>  summedMRPairs2 = summedMRPairs.mapToPair(s->(new Tuple2<>(s._1(), generateStringList(s._2()))));
		JavaRDD<List<String>> connectedIds = summedMRPairs2.map(s->s._2());
		JavaRDD<List<String>> connectedIds2=connectedIds.map(s->generatePairs(s));
		JavaRDD<String> nodes = connectedIds2.flatMap(s->(s.iterator()));
		JavaPairRDD<String, Integer> edges = nodes.mapToPair(s->new Tuple2<>(s, 1));
		JavaPairRDD<String, Integer> summedEdges = edges.reduceByKey((i1, i2) -> i1 + i2);
		List<Tuple2<String, Integer>> output = summedEdges.collect();
		for (Tuple2<?,?> tuple : output) {
			if(Integer.parseInt(tuple._2().toString())>1)
		      System.out.println(tuple._1() + " " + tuple._2());
		    }
		System.out.println(6);
		/* Implement Here */ 
		
	}
	

}
