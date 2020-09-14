package com.virtualpairprogrammers.streaming;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

import static com.virtualpairprogrammers.StanfordSentiment.*;

import java.time.LocalDateTime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.bson.BSONObject;
import org.bson.Document;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

public class Functions {
	
   static MongoClientURI uri = new MongoClientURI(
      // "mongodb+srv://acamitic7788:mitic1305@cluster0.glbwx.gcp.mongodb.net/test?retryWrites=true&w=majority");
		   "mongodb+srv://acamitic7788:mitic1305@cluster0.c8qeh.gcp.mongodb.net/test?retryWrites=true&w=majority");
   //"mongodb+srv://acamitic7788:mitic1305@cluster0.c8qeh.gcp.mongodb.net/<dbname>?retryWrites=true&w=majority");
    static MongoClient mongo = new MongoClient(uri);


	//static MongoClient mongo = new MongoClient("localhost", 27017);
    static MongoDatabase db = mongo.getDatabase("test");
    static MongoCollection<Document> collection = db.getCollection("sport");
    
	// funkcija koja vrsi sentiment analizu prosledjenog pojma
	public static String SentimentAnalysis(String row, String collection_name)	
	{
		//getCollection(collection_name);
		LocalDateTime lv_timestamp;
		String lv_timestamp_string;
      
		String ReturnValue = "";
		//JSONObject jsonObject = new JSONObject(row);
		JsonObject obj = new JsonParser().parse(row).getAsJsonObject();
	
		String body = obj.get("body").getAsString();
		
		double sentValue = GetSentimentValue(body);		
		if(sentValue  > 0)	
		{
			if(sentValue  <= 1)
				ReturnValue = "Positive";
			else
				ReturnValue = "Very Positive";
		}
		else if(sentValue  == 0)
		{
			ReturnValue = "Neutral";
		}
		else
		{
			if(sentValue  < -1)
				ReturnValue = "Very negative";
			else
				ReturnValue = "Negative";				
		}
		lv_timestamp = java.time.LocalDateTime.now();
		lv_timestamp_string = lv_timestamp.toString();
		obj.addProperty("current_timestamp", lv_timestamp_string);								// Vreme analize
		obj.addProperty("sentiment_value", ReturnValue);								 		// Sentiment vrednost analize string
		obj.addProperty("sentiment_value_num", sentValue);										// Sentiment vrednost analize numeric
		DBObject dbObject = (DBObject) JSON.parse(obj.toString());
		
	    System.out.println(obj.toString());
        Document document = new Document(dbObject.toMap());                       

        collection.insertOne(document);
		
		return obj.toString();
		
		 
	}
		

	private static void getCollection(String collection) {
		if(Functions.collection == null)
		{
			db.getCollection(collection);
		}
		
	}

	public static boolean isCategory(String message, String kategorija)
	{
		if (kategorija != null)
		{
			int index_f;
			int length;
			String category;
			if(message.startsWith("subreddit") && !message.startsWith("subreddit_id")) 
			{
				index_f = message.indexOf(":");
				length = message.length();
				category = message.substring(index_f + 1, length);
				if(category.startsWith(kategorija))			
					return true;
				else
					return false;
			}
			else 
			{
				return false;
			}
		}
		else
			return true;
	}

	// Vraca true ukoliko je prosledjena kolona kategorija
	public static boolean isCategory(String word)
	{
		if (word.startsWith("subreddit") && !word.startsWith("subreddit_id"))
			return true;
		else
			return false;
	}
	
	// Prosledjuje se kateforija:nazivKategorije
	// Funkcija vraca samo nazivKategorije posle :
	public static String getCategoryName(String column)
	{
		int index = column.indexOf(":");
		int length = column.length();
		return column.substring(index + 1, length);
		
	}
	
	// Vraca true ukoliko se radi o autoru, o suprotnom vraca false
	public static boolean isAuthor(String column)
	{
		if(column.startsWith("author") && !column.startsWith("author_flair_css_class") && !column.startsWith("author_flair_text"))
			return true;
		else
			return false;
	}
	
	// Vrati true ukoliko ime Autora nije izbrisano	
	public static boolean AuthorsWithName(String author)
	{
		int index = author.indexOf(":");
		int length = author.length();
		String name = author.substring(index + 1, length);
		
		if(!name.startsWith("[deleted]"))		// ukoliko ime autora nije izbrisano
			return true;						// vrati true
		else
			return false;						// u suprotnom vrati false
	}
	
	// Vraca naziv autora
	public static String getAuthorName(String author)
	{
		int index = author.indexOf(":");
		int length = author.length();
		return  author.substring(index + 1, length);		// vrati ime autora
		
		
	}
	
	// Vracamo true ukoliko se radi o koloni koja predstavlja poruku koju je korisnik ostavio na Reddit sajtu
	public static boolean isMessage(String column)
	{
		if(column.startsWith("body"))
			return true;
		else
			return false;
	}
	public static boolean isNotDeletedMessage(String row)
	{
		JsonObject obj = new JsonParser().parse(row).getAsJsonObject();
		String message = obj.get("body").getAsString();
		
		if(!message.startsWith("[deleted]"))
			return true;
		else
			return false;
	}
	
	public static boolean containsKeyWord(String row, String word)
	{
		JsonObject obj = new JsonParser().parse(row).getAsJsonObject();
		String message = obj.get("body").getAsString();
		
		message = message.toLowerCase();
		
		if(message.contains(word))
			return true;
		else
			return false;
	}
	
	public static boolean inCategory(String row, String word)
	{
		if(word != "/")
		{	
			JsonObject obj = new JsonParser().parse(row).getAsJsonObject();
			String message = obj.get("subreddit").getAsString();
			
			message = message.toLowerCase();
			
			if(message.startsWith(word))
				return true;
			else
				return false;
		}
		else
		{
			return true;	// obradi sve kategorije
		}
	}
		
	
	// Vraca sadrzaj komentara koji je korisnik ostavio na reddit sajtu
	public static String getMessage(String column)
	{
		int index = column.indexOf(":");
		int length = column.length();
		return column.substring(index + 1, length);		// vrati ime tekst poruke samo
	}
	
	// Da li je kolona LIKE(UPS)
	public static boolean isLike(String column)
	{
		if(column.startsWith("ups"))
			return true;
		else
			return false;
	}
	
	public static String getLike(String column)
	{
		int index = column.indexOf(":");
		int length = column.length();
		return column.substring(index + 1, length);		// vrati ime tekst poruke samo
	}
	
	public static boolean isLikeOrMessage(String column)
	{
		if(column.startsWith("ups") || column.startsWith("body"))
			return true;
		else
			return false;
		
	}

	//vraca broj lajkova
	public static int getLikesFromRow(String row) {
		int indexOne = row.lastIndexOf("ups\":");		
		int indexSecond = row.indexOf(",", indexOne + 1);
		if (indexSecond < 0)
			indexSecond = row.indexOf("}", indexOne + 1);
		return Integer.parseInt(row.substring(indexOne+5,indexSecond));
	}

	//Vraca naziv poruke
	public static String getMessageFromRow(String row) {
		int indexOne = row.lastIndexOf("body\":");			  // nadji pocetak poruke
		int indexSecond = row.indexOf("\",", indexOne + 1);	  // nadji kraj poruke
		if (indexSecond < 0)								  // ukoliko nije pronadjen indeks koji oznacava kraj poruke
			indexSecond = row.indexOf("\"}", indexOne + 1);   // trazi kraj poruke ovako "}
		return row.substring(indexOne + 6, indexSecond + 1);  // vrati sadrzaj poruke
	}
	
	// vraca naziv kategorije
	public static String getCategoryFromRow(String row) {
		int indexOne = row.lastIndexOf("subreddit\":");			  // nadji pocetak poruke
		int indexSecond = row.indexOf("\",", indexOne + 1);	  // nadji kraj poruke
		if (indexSecond < 0)								  // ukoliko nije pronadjen indeks koji oznacava kraj poruke
			indexSecond = row.indexOf("\"}", indexOne + 1);   // trazi kraj poruke ovako "}
		return row.substring(indexOne + 11, indexSecond + 1);  // vrati sadrzaj poruke
	}
	// vraca naziv autora
	public static String getAuthorFromRow(String row) {
		int indexOne = row.lastIndexOf("author\":");			  // nadji pocetak poruke
		int indexSecond = row.indexOf("\",", indexOne + 1);	  // nadji kraj poruke
		if (indexSecond < 0)								  // ukoliko nije pronadjen indeks koji oznacava kraj poruke
			indexSecond = row.indexOf("\"}", indexOne + 1);   // trazi kraj poruke ovako "}
		return row.substring(indexOne + 8, indexSecond + 1);  // vrati sadrzaj poruke
	}
	
	public static String getCatAndMessageAndAuthorFromRow(String row) {			
		String category = getCategoryFromRow(row);
		String message = getMessageFromRow(row);
		String author = getAuthorFromRow(row);
		
		return category + "," + message + "," + author;
	}


}
