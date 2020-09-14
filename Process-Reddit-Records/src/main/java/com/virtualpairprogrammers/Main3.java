package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class Main3 {

	//Level 6
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

		
		JavaRDD<String> originalLogMessages = sc.parallelize(inputData); 
		
		//Kako bismo razvojili level upozorenja i poruku koristimo MAPTOPAIR		
         JavaPairRDD<String, Long> pairRDD = originalLogMessages.mapToPair(rawValue -> { //Za svaki red u input data 
			String[] columns = rawValue.split(":");  //Splitujemo na :
			String level = columns[0];				// level je upozorenje
			//String date = columns[1];			
			
			return new Tuple2<>(level,1L);	// Vracamo vrednosti level i 1
		});
         
         // radimo redukciju za svaki level
         JavaPairRDD<String,Long> sumRDD = pairRDD.reduceByKey((value1, value2) -> value1 + value2);
         //tuple ima 2 vrednosti level i sumu, stampamo ih
         sumRDD.foreach(tuple -> System.out.println(tuple._1 + " ima " + tuple._2 + "instance"));
		
         
         //isto resenje preko group by key ali ne koristiti group by ukoliko ne moramo
         JavaPairRDD<String,Iterable<Long>> sumRDD2 = pairRDD.groupByKey();
         
         sumRDD2.foreach(tuple -> System.out.println(tuple._1 + " ima " + Iterables.size(tuple._2) + "instance"));
		sc.close();


	}

}
