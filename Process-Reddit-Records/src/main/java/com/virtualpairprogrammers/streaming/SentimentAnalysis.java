package com.virtualpairprogrammers.streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.bson.Document;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

public class SentimentAnalysis {
	static String pojam = "";
	static float vreme_analize;
	static long vreme_analize_milliseconds;
	static String kategorija;
	
	public static void main(String[] args) {		
		Scanner scan = new Scanner(System.in);
		System.setProperty("hadoop.home.dir", "c:/hadoop");	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("viewRedditRecords")
		.set("spark.driver.maxResultSize", "21g")
		.set("spark.executor.heartbeatInterval", "200000")
		.set("spark.network.timeout", "300000");
		
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));	// svake sekunde se rade izracunavanja
		
		Collection<String> topics = Arrays.asList("viewRedditRecords");
		
		Map<String, Object> params = new HashMap<>();
		params.put("bootstrap.servers", "localhost:9092");
		params.put("key.deserializer", StringDeserializer.class);
		params.put("value.deserializer", StringDeserializer.class);
		params.put("group.id", "spark-group");
		params.put("auto.offset.reset", "latest");
		params.put("enable.auto.commit", true);
		
		// povezivanje
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(), 
						                      ConsumerStrategies.Subscribe(topics, params));	// da bismo se povezali, prosledjujemo kao topics nazive koje smo kreirali (kom. kanale)
		
		System.out.println("Unesite pojam koji želite da analizirate:");
		pojam = scan.next();
		
		System.out.println("(opciono) Unesite kategoriju u kojoj želite da pretražujete pojam");
		kategorija = scan.next();
		
		System.out.println("Unesite vreme trajanja analize u satima:");
		vreme_analize = scan.nextFloat();
		
		System.out.print("Pojam koji želite da analizirate " + vreme_analize + " sati je " + pojam + "\n");
		scan.close();
		System.out.print("Pocetak obrade...\n");
		
		vreme_analize_milliseconds = (long) (vreme_analize * 60 * 60 * 1000);	// Pretvaramo sate u milisekunde
		pojam = pojam.toLowerCase();		
		
		JavaDStream<String> results = stream.map(item -> item.value())
				.filter(row -> Functions.isNotDeletedMessage(row))
				.filter(cat -> Functions.inCategory(cat, kategorija.toString()))
				//.filter(message_category -> Functions.isCategory(message_category, kategorija))
				.filter(message -> Functions.containsKeyWord(message, pojam.toString()));
		
		JavaDStream<String> rdd = results.map(row -> Functions.SentimentAnalysis(row.toString(), pojam.toString()))
										.window(Durations.minutes(60), Durations.minutes(1));

		rdd.print(15000);
		

		sc.start();
		try {
			//sc.awaitTermination();
			sc.awaitTerminationOrTimeout(vreme_analize_milliseconds);		// analiza traje u dok ne istece timeout ili dok ne prekinemo analizu
			System.out.print("Kraj obrade.");
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		
	}
		//sc.stop();
	}

}
