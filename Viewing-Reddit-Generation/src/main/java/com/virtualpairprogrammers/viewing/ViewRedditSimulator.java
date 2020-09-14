package com.virtualpairprogrammers.viewing;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ViewRedditSimulator {

	public static void main(String[] args) throws FileNotFoundException, InterruptedException {
		int rand_int1;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all"); // See https://kafka.apache.org/documentation/
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");						// prosledjujemo kljuc zato ga serializujemo u string
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");					// i prosledjujemo value naziv kursa i njega serializujemo kao string
		// preko kafka prosledjujemo kao key naziv kom. kanala i vrednost jednog reda kao value

		Producer<String, String> producer = new KafkaProducer<>(props);		
	    //Citamo fajl sa diska
		Scanner sc = new Scanner(new FileReader("F:\\Diplomski\\RC_2015-01\\RC_2015-01"));
	    int milliseconds = 0;
	    int timestamp = 24;
	    Random rand = new Random();
	    // setujemo } kao delimiter jer to oznacava kraj jednog reda
	    sc.useDelimiter("}"); 
	    while (sc.hasNextLine())	      		// za svaki red/poruku iz fajla
		{

	    	rand_int1 = rand.nextInt(40); 		// generisemo random broj do 40
	    	if (rand_int1 < 24);		  		// ukoliko je manji od 24 setujemo da bude minimalni 24
	    	  rand_int1 = 24;
	    	  
	    	timestamp += rand_int1; 	 		// uvecavamo timestamp
	    	
			while (milliseconds < timestamp)	// sve dok je broj milisekunde manji od timestampa
			{
				milliseconds++;					// uvecavamo brojac
				if (milliseconds % 24 == 0) {   // ukoliko po modulu 24 daje nulu
					Thread.sleep(100);          // stavljamo trenutnu nit na sleep
				}
			}
			// saljemo poruku na kanal viewRedditRecords
	    	producer.send(new ProducerRecord<String, String>("viewRedditRecords", sc.nextLine()));			// i prosledjujemo naziv kursa za generisani broj na kanal viewrecords
		}
		sc.close();
		producer.close();



	}

}
