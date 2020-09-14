package com.virtualpairprogrammers.streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class AuthorWithTheMostComments {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("viewRedditRecords");
		
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
		 
		JavaDStream<String> results = stream.map(item -> item.value());

		// Razdvajamo na kolona: vrednost
		JavaDStream<String> list = results.flatMap(sentence -> Arrays.asList(sentence.split(",")).iterator());
		list = list.map(item -> item.replaceAll("\"", ""));	// Brisemo navodnike svakoj koloni
		
		list = list.filter(column -> Functions.isAuthor(column))	// vracamo samo autore
		.map(item -> item.replaceAll("}", ""))				       // Brisemo } svakom autoru
		.filter(author -> Functions.AuthorsWithName(author));	   // i autore cija imena nisu izbrisana				
		
	    JavaPairDStream<Long, String> authors = list.mapToPair(category -> new Tuple2<>(Functions.getAuthorName(category),1L))
	    		.reduceByKeyAndWindow((x,y) -> x + y, Durations.minutes(60), Durations.minutes(1))  // ispis u konzolu na svaki minut, ali se izracunavanja rade svake sekunde
				.mapToPair(item -> item.swap())								// swapujemo kolone
				.transformToPair(rdd -> rdd.sortByKey(false));				// sortiramo u descending
	    
	    authors.print(10);
		
		sc.start();
		try {
			sc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		 
	}

}
