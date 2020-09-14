package com.virtualpairprogrammers;


import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main5 {
//Level 8 read from disk, ne sa lokalnog hardiska vec sa diska gde mogu da stanu velike kolicine podataka
	// na hard disku ne mogu da stanu ti podaci tog reda koje spark moze da obradjuje
	// ali na ovom primeru ce biti prikazani podaci iz lokalnog fajla
	//Level 9> citamo isti fajl, i trazimo rec koja se najvise ponavlja
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/hadoop");	//potavljamo putanju do hadoop fajla zbog exceptiona prilikom citanja txt fajla na windowsu
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// da je ostalo samo local to bi se samo jedna nit izvrsavala dodatno objasnjenje kasnije
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		//komunikacija app sa Sparkom 
		JavaSparkContext sc = new JavaSparkContext(conf);

		// citamo podatke za obradu sa nekog servera, sa amazona npr
		// citamo lokalni fajl input.txt
		JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
		
		JavaRDD<String> lettersOnlyRDD = initialRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase()); // sve sto je u zagradama a nisu slova ili razmak zameniti blanko znakom
		//\\s - predstavlja blanko znak
		
		// brisemo prazne linije
		JavaRDD<String> removedBlankLines = lettersOnlyRDD.filter(sentence -> sentence.trim().length() > 0);
		// razdvarajmo samo reci u redove
		JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
		
		//izdvajamo samo one reci koje se nalaze u listi
		JavaRDD<String> justInterestingWords = justWords.filter(word -> Util.isNotBoring(word));
		
		//za svaku rec vrati mi broj 1
		JavaPairRDD<String, Long> pairRDD = justInterestingWords.mapToPair(word -> new Tuple2<String,Long>(word,1L));
		
		JavaPairRDD<String,Long> totals = pairRDD.reduceByKey((value1,value2) -> value1 + value2);
		//nakon izracunavanja, menjamo redosled key i value
		JavaPairRDD<Long,String> swithced = totals.mapToPair(tuple -> new Tuple2<Long,String>(tuple._2,tuple._1));
		
		JavaPairRDD<Long,String> sorted = swithced.sortByKey(false);
		
		List<Tuple2<Long,String>> result = sorted.take(50);
		result.forEach(row ->System.out.println(row)); 
		
		sc.close();

	}

}
