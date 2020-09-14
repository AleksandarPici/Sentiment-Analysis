package com.virtualpairprogrammers.streaming;

import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.Function;
import static com.virtualpairprogrammers.StanfordSentiment.*;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.bson.Document;

public class Sentiment {

	public static void main(String[] args) throws StreamingQueryException {
		//System.out.println("Rec koju analiziramo je: " + args[0]);
		System.setProperty("hadoop.home.dir", "c:/hadoop");	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkSession session = SparkSession.builder()
				.master("local[*]")
				.appName("structuredViewingReport")
			    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.reddit")  //odakle da citamo podatke iz koje baze i kolekcije
			    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.reddit")	 // gde smestamo podatke 
				.getOrCreate();	
		
		JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());		// Java context
		// Create a custom WriteConfig
	    Map<String, String> writeOverrides = new HashMap<String, String>();
	    writeOverrides.put("collection", "sentiment");
	    writeOverrides.put("writeConcern.w", "majority");
	    WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
		
	    session.conf().set("spark.sql.shuffle.partitions", "10");
		session.sqlContext().udf().register("Sentiment", (String s) -> GetSentimentValue(s), DataTypes.DoubleType);
		
		Dataset<Row> df = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")		// postavljamo kafka server
                .option("subscribe", "viewRedditRecords,sentimentrecords")					// postavljamo se na kanal "viewRedditRecords"                
                //.option("subscribe", "sentimentrecords")
                .load()
                .selectExpr("CAST(value AS STRING)");
				
			Dataset<RedditAtributesRow> dataset = df.as(Encoders.STRING()).map(x -> {
				ObjectMapper mapper = new ObjectMapper();
				return mapper.readValue(x, RedditAtributesRow.class);

			}, Encoders.bean(RedditAtributesRow.class));
		 
			Dataset<Row> dataset2  = dataset.withColumn("current_timestamp", functions.current_timestamp());
			dataset2.createOrReplaceTempView("reddit_details");
			
/*			Dataset<Row> dataset3 = session.sql("select * from reddit_details where body regexp '(" + args[0] + ")'");
			dataset3.createOrReplaceTempView("reddit");
	*/		
			Dataset<Row> result = session.sql("select current_timestamp, subreddit, body, Sentiment(body) as seVal from reddit_details where body != '[deleted]' and author != '[deleted]' group by current_timestamp, subreddit, body")
				.orderBy(org.apache.spark.sql.functions.col("seVal").desc());
		

			//MongoSpark.write(result).option("collection", "sentiment").save();
			
			//and body regexp '(" + args[0] + ")' 
			//window(current_timestamp,'2 minutes')
			/*JavaRDD<Document> sparkDocuments = result.toJavaRDD().map(new Function<Row, Document>() {
			       public Document call(Row row) throws Exception {
		               return Document.parse("{spark: " + row.getClass() + "}");
		             }
		      });*/
			//JavaRDD<Row> sparkDoc = result.toJavaRDD();
			// MongoSpark.save(sparkDocuments, writeConfig);
			
			StreamingQuery query = result	
			.writeStream()
			.format("console")								// pored "console" mozemo podatke smestiti u nekom fajlu .csv, .json...			
			.outputMode(OutputMode.Complete())				// Complete() - prikazuje rezultat u jednu celu tabelu, kako imamo agregaciju po imenu kursa tabela ce biti ogranicena na 42 reda
		    												// Update() - prikazanu tabelu konstanto azurira kada dodje do promene. Poredi tabele iz prethodnog batch-a i azurira ih ukoliko su promene nastale. Ukoliko promena nije nastala slog se ne pojavljuje u sledecem batch-u
															// Append()
			//.option("truncate", false)						// da se vidi cela vrednost u koloni
			//.option("numRows", 50)						    // max 50 reda da se prikazu
			//.foreach(new KafkaWriter())
			.start();
	

			
		query.awaitTermination();
			
		/*
		/*Start Example: Save data from RDD to MongoDB*****************/
		//MongoSpark.save(result, writeConfig);
		    /*End Example**************************************************/
		//System.out.print("Kraj");
		 jsc.close();

	}

}
