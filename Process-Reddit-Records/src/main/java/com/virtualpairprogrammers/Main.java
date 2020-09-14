package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
// od level 1 do level 4
	public static void main(String[] args) 
	{
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// da je ostalo samo local to bi se samo jedna nit izvrsavala dodatno objasnjenje kasnije
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		//komunikacija app sa Sparkom 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//ovaj objekat JAVARDD kao i ostale koje cemo koristii su kreirani u programu SCALA
		JavaRDD<Integer> myRdd = sc.parallelize(inputData); // trenutno nista ne radimo sa podacima, samo smo kreirali execution plan
		
		//Posto je myRdd radi sa Double tipovima ne moramo da ih naglasavamo za value1 i value2
		Integer result = myRdd.reduce((value1,value2) -> value1 + value2);	//prosledjujemo funkciju koja sabira 2 vrednosti
		System.out.println(result);	//rezultat redukcije
		
		// Mapiranje moze da vrati bilo koji tip
		JavaRDD<Double> sqrtRDD = myRdd.map(value -> Math.sqrt(value));
		
		sqrtRDD.foreach(value -> System.out.println(value)); //rezultat mapiranja, stamapmo koren svakog broja
		
		//koliko elemenata imamo u sqrtRDD
		System.out.println("Ukupno elemenata u sqrtRDD:" + sqrtRDD.count());
		
		//koliko elemenata imamo u sqrtRDD samo uz pomoc Map i Reduce
		//Mapiramo svaku vrednos u jedinice i odradimo redukciju nad jedinicama
		JavaRDD<Long> singleIntegerRdd = sqrtRDD.map(value -> 1L);	//Za svaku vrednost vrati broj 1
		Long count = singleIntegerRdd.reduce((value1,value2) -> value1 + value2);
		System.out.println("Ukupno elemenata u sqrtRDD MAP/REDUCE:" + count);
		
		sc.close();
	}

}
