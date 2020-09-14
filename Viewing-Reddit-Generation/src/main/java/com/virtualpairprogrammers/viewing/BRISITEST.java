package com.virtualpairprogrammers.viewing;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.virtualpairprogrammers.streaming.RedditAtributesRow;

public class BRISITEST {

	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
		// TODO Auto-generated method stub
		Scanner sc = new Scanner(new FileReader("E:\\Diplomski\\RC_2015-01\\RC_2015-01"));
		 sc.useDelimiter("}"); 
		 int line = 0;
		 while (sc.hasNextLine())
		{	
			 /*String[] input = sc.nextLine().split(",");
		    	for (String in : input) {
		    		in = in.replace("\"", "");
		    		int index = in.indexOf(":");
		    		int length = in.length();
		    		System.out.println(in);
		    		if (in.startsWith("subreddit") && !in.startsWith("subreddit_id"))
			    	   //sc.findInLine("score_hidden");
			    	 System.out.println(in.substring(index + 1, length));
		    		
		    	}*/
			 
			 // like ups
			 /*String input = sc.nextLine();
			 int index = input.indexOf("ups\":");
			 int second = input.indexOf(",", index + 1);
			 if(second < 0)
				 second = input.indexOf("}",index + 1);
			 
				 
			 String likes = input.substring(index+5,second);
			 System.out.print(likes + "\n");
			 String nista = "";*/
			 
			 // Message
			 /*String input = sc.nextLine();
			 int index = input.indexOf("body\":");
			 int second = input.indexOf("\",", index + 1);
			 if(second < 0)
				 second = input.indexOf("\"}",index + 1);
			 
			 line = line + 1;
			 String Message = input.substring(index+6,second + 1);
			 System.out.print(line + Message + "\n");
			 String nista = "";*/
			 
			 // Category
			 /*String input = sc.nextLine();
			 int index = input.indexOf("subreddit\":");
			 int second = input.indexOf("\",", index + 1);
			 if(second < 0)
				 second = input.indexOf("\"}",index + 1);
			 
			 line = line + 1;
			 String Category = input.substring(index+11,second +1);
			 System.out.print(line + Category + "\n");
			 String nista = ""; */
			 
			 // Author			 
			 /*String input = sc.nextLine();
			 int index = input.indexOf("author\":");
			 int second = input.indexOf("\",", index + 1);
			 if(second < 0)
				 second = input.indexOf("\"}",index + 1);
			 
			 line = line + 1;
			 String Author = input.substring(index+8,second +1);
			 System.out.print(line + Author + "\n");
			 String nista = "";*/
			 
			 //deserialization
		     ObjectMapper mapper = new ObjectMapper();
		     RedditAtributesRow car = mapper.readValue(sc.nextLine(), RedditAtributesRow.class);
		     System.out.println(car.getUps());
		     System.out.println(car.getSubreddit());
			 
		}
	}

}
