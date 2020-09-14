package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class Main4 {
//level 7
	public static void main(String[] args) {
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Sreda 4 Septembar 2020");
		inputData.add("ERROR: Utorak 4 Septembar 2021");
		inputData.add("WARN: Cetvrtak 5 Septembar 0021");	
		inputData.add("ERROR: Utorak 4 Septembar 2045");
		inputData.add("FATAL: Poneteljak 1 Septembar 2021");
		inputData.add("WARN: Sreda 2 Septembar 1111");

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// da je ostalo samo local to bi se samo jedna nit izvrsavala dodatno objasnjenje kasnije
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		//komunikacija app sa Sparkom 
		JavaSparkContext sc = new JavaSparkContext(conf);

		
       sc.parallelize(inputData)
       	 .flatMap(value -> Arrays.asList(value.split(" ")).iterator())	// podeli recenicu na reci
       	 .filter(word -> word.length() > 1)								// prikazi samo reci koje imaju vecu duzinu od 1
       	 .foreach(value -> System.out.println(value));					//stampaj reci
		
		
		sc.close();
	}

}
