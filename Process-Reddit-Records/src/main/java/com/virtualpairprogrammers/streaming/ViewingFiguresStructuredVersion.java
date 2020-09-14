package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class ViewingFiguresStructuredVersion {

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
                .option("subscribe", "viewrecords")							// postavljamo se na kanal viewrecords
                .load();
		
		// start some dataframe operations
		df.createOrReplaceTempView("viewing_figures");					// kreiramo temp. view odnosno tabelu
		
		// key, value, timestamp
		Dataset<Row> result = 
				session.sql("select window, cast (value as string) as course_name, sum(5) as seconds_watched from viewing_figures group by window(timestamp,'2 minutes'), course_name"); // order by seconds_watched desc");
		
		StreamingQuery query = result
			.writeStream()
			.format("console")								// pored "console" mozemo podatke smestiti u nekom fajlu .csv, .json...
			.outputMode(OutputMode.Complete())				// Complete() - prikazuje rezultat u jednu celu tabelu, kako imamo agregaciju po imenu kursa tabela ce biti ogranicena na 42 reda
		    												// Update() - prikazanu tabelu konstanto azurira kada dodje do promene. Poredi tabele iz prethodnog batch-a i azurira ih ukoliko su promene nastale. Ukoliko promena nije nastala slog se ne pojavljuje u sledecem batch-u
															// Append()
			.option("truncate", false)						// da se vidi cela vrednost u koloni
			.option("numRows", 50)						    // max 50 reda da se prikazu
			.start();
		
		
		query.awaitTermination();
		
		
	}

}
