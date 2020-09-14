package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AnnoyingUsersStructured {

	public static void main(String[] args) throws StreamingQueryException {
		System.setProperty("hadoop.home.dir", "c:/hadoop");	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkSession session = SparkSession.builder()
				.master("local[*]")
				.appName("structuredViewingReport")
				.getOrCreate();
		
		session.conf().set("spark.sql.shuffle.partitions", "10");
		
		Dataset<Row> df = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")		// postavljamo kafka server
                .option("subscribe", "viewRedditRecords")					// postavljamo se na kanal "viewRedditRecords"                
                .load()
                .selectExpr("CAST(value AS STRING)");
				
			Dataset<RedditAtributesRow> dataset = df.as(Encoders.STRING()).map(x -> {
				ObjectMapper mapper = new ObjectMapper();
				return mapper.readValue(x, RedditAtributesRow.class);

			}, Encoders.bean(RedditAtributesRow.class));
		 
			Dataset<Row> dataset2  = dataset.withColumn("current_timestamp", functions.current_timestamp());
			dataset2.createOrReplaceTempView("reddit_details");
			
			
		   Dataset<Row> result = 
				   session.sql("select window, author, sum(ups) as dislikes from reddit_details where ups < 0 and author != '[deleted]' group by window(current_timestamp, '2 minutes'), author, ups")//group by window(current_timestamp, '2 minutes')") //ups			   
				   .orderBy(org.apache.spark.sql.functions.col("dislikes").asc());
		  // cast (ups as string) as likes
		  // count(body) as messages
		  //  sum(1) as messages, 
	
		StreamingQuery query = result
			.writeStream()
			.format("console")								// pored "console" mozemo podatke smestiti u nekom fajlu .csv, .json...
			.outputMode(OutputMode.Complete())				// Complete() - prikazuje rezultat u jednu celu tabelu, kako imamo agregaciju po imenu kursa tabela ce biti ogranicena na 42 reda
		    												// Update() - prikazanu tabelu konstanto azurira kada dodje do promene. Poredi tabele iz prethodnog batch-a i azurira ih ukoliko su promene nastale. Ukoliko promena nije nastala slog se ne pojavljuje u sledecem batch-u
															// Append()
			.option("truncate", false)						// da se vidi cela vrednost u koloni
			//.option("numRows", 50)						    // max 50 reda da se prikazu
			.start();
		
		
		query.awaitTermination();
		

	}

}
