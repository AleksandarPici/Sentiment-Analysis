package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main2 {
	//Level 5
	public static void main(String[] args) {
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
		
		//Imamo 2 skupa podataka 2 RDD i i hocemo da ih spojimo
		JavaRDD<Integer> originalIntegers = sc.parallelize(inputData); 
		//moze ovako preko nove klase koju mi definisemo i prosledimo je
		JavaRDD<IntegerWithSquareRoot> sqrtRDD = originalIntegers.map(value -> new IntegerWithSquareRoot(value));
		
		//Ili mozemo preko Tuples da spojimo
		Tuple2<Integer,Double> myValue = new Tuple2<Integer, Double>(9,3.0);	//importujemo tuple2 iz scale, ne spada u javu ali preko Apache biblioteke mozemo ga koristiti nezavisno
		
		//Dakle gornji primer sqrtRDD mozemo zapisati i ovako:
		JavaRDD<Tuple2<Integer,Double>> sqrtRDD2 = originalIntegers.map(value -> new Tuple2(value,Math.sqrt(value)));
		// veoma se cesto koriste u sparku, laka je sintaksa ne moraju da se definisu
		
		//new Tuple5(4,3,2,1,0);
		//new Tuple22()				Moguce je definisati 22 kolone
		sc.close();

	}

}
