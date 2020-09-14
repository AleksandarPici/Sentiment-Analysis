package com.virtualpairprogrammers.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.bson.Document;
import org.slf4j.Logger;

//import com.google.gson.JsonObject;
import com.mongodb.spark.MongoSpark;
import com.google.gson.*;

import org.apache.spark.streaming.Duration;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StreamToMongoTest {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		SparkSession sparkSession = SparkSession.builder()
		        .appName("Mongo_test")
		        .master("local[*]")
		        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.spark")
		        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.spark")
		        .getOrCreate();
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext,new Duration(20000));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		/*kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spark-group");// stream-wise unique
		kafkaParams.put("enable.auto.commit", true);
		kafkaParams.put("auto.offset.reset", "earliest");*/
		
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spark-group");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", true);
		
		Collection<String> topics = Arrays.asList("viewRedditRecords");
		
		JavaInputDStream<ConsumerRecord<String, String>> javaInputDStream = KafkaUtils.createDirectStream(javaStreamingContext,LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics,kafkaParams));
		javaInputDStream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> rdd) ->{
		    OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd.rdd()).offsetRanges();
		    JavaRDD<JsonObject> jsonObjectJavaRDD = rdd.map(record -> {
		        JsonObject jsonObject = null;
		        try{
		            jsonObject = new Gson().fromJson(record.value(), JsonObject.class);
		        }catch (Exception e){
		            //Logger.error("Failed to create JSON for the entry: {}", record.value()); 
		        	System.out.println("Failed to create JSON for the entry: {}" + record.value());
		        }

		        return jsonObject;
		    });
		    
		    jsonObjectJavaRDD = jsonObjectJavaRDD.filter(Objects::nonNull);// remove empty records
		    JavaRDD<Document> dbEntry = jsonObjectJavaRDD.map(record -> {
		    	Document document = new Document();
		    	document.put("key",(record.get("value_1")).getAsString());// this will change according to your json format
		  
		    return document;
		});
		    
		    //try{
		        MongoSpark.save(dbEntry);
		        System.out.println("Kraj");
		    //}catch (Exception e){
		        //LOGGER.error("Failed to write into database: {}" , e.getMessage());
		    	//System.out.println("Failed to write into database: {}" + e.getMessage());
		    //}
		    ((CanCommitOffsets)javaInputDStream.inputDStream()).commitAsync(offsetRanges);});

	}

}
