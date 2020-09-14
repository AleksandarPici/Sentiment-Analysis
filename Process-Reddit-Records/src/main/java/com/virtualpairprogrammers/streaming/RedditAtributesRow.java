package com.virtualpairprogrammers.streaming;

import java.sql.Timestamp;

public class RedditAtributesRow {
	private Integer ups;					// broj lajkova
	private String body;					// sadrzaj
	private String score_hidden;			// score
	private String name;					// ime
	private String link_id;					// id linka 
	private String downs;					// broj dislajkova
	private String created_utc;				// vreme kreiranja posta
	private Integer score;					// pravi score
	private String author;					// ime autora (nickname)
	private String distinguished;			// pregledan
	private String id;						// id posta
	private String archived;				// broj arhiviranja od strane drugih kor.
	private String parent_id;				// id kategorije
	private String subreddit;				// naziv kategorije
	private String author_flair_css_class;	
	private String author_flair_text;
	private String gilded;					// golden user
	private String retrieved_on;			// datum preuzimanja
	private String controversiality;		// kontroverzan
	private String subreddit_id;			// id kategorije
	private String edited;					// da li je izmenjen komentar
	private Timestamp timestamp;			// vreme obrade
	
	public String getBody() {
		return body;
	}


	public void setBody(String body) {
		this.body = body;
	}


	public Integer getUps() {
		return ups;
	}


	public void setUps(Integer ups) {
		this.ups = ups;
	}


	public String getScore_hidden() {
		return score_hidden;
	}


	public void setScore_hidden(String score_hidden) {
		this.score_hidden = score_hidden;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getDowns() {
		return downs;
	}


	public void setDowns(String downs) {
		this.downs = downs;
	}


	public String getLink_id() {
		return link_id;
	}


	public void setLink_id(String link_id) {
		this.link_id = link_id;
	}


	public String getCreated_utc() {
		return created_utc;
	}


	public void setCreated_utc(String created_utc) {
		this.created_utc = created_utc;
	}


	public Integer getScore() {
		return score;
	}


	public void setScore(Integer score) {
		this.score = score;
	}


	public String getAuthor() {
		return author;
	}


	public void setAuthor(String author) {
		this.author = author;
	}


	public String getDistinguished() {
		return distinguished;
	}


	public void setDistinguished(String distinguished) {
		this.distinguished = distinguished;
	}


	public String getId() {
		return id;
	}


	public void setId(String id) {
		this.id = id;
	}


	public String getArchived() {
		return archived;
	}


	public void setArchived(String archived) {
		this.archived = archived;
	}


	public String getParent_id() {
		return parent_id;
	}


	public void setParent_id(String parent_id) {
		this.parent_id = parent_id;
	}


	public String getSubreddit() {
		return subreddit;
	}


	public void setSubreddit(String subreddit) {
		this.subreddit = subreddit;
	}


	public String getAuthor_flair_css_class() {
		return author_flair_css_class;
	}


	public void setAuthor_flair_css_class(String author_flair_css_class) {
		this.author_flair_css_class = author_flair_css_class;
	}


	public String getAuthor_flair_text() {
		return author_flair_text;
	}


	public void setAuthor_flair_text(String author_flair_text) {
		this.author_flair_text = author_flair_text;
	}


	public String getGilded() {
		return gilded;
	}


	public void setGilded(String gilded) {
		this.gilded = gilded;
	}


	public String getRetrieved_on() {
		return retrieved_on;
	}


	public void setRetrieved_on(String retrieved_on) {
		this.retrieved_on = retrieved_on;
	}


	public String getSubreddit_id() {
		return subreddit_id;
	}


	public void setSubreddit_id(String subreddit_id) {
		this.subreddit_id = subreddit_id;
	}


	public String getControversiality() {
		return controversiality;
	}


	public void setControversiality(String controversiality) {
		this.controversiality = controversiality;
	}


	public String getEdited() {
		return edited;
	}


	public void setEdited(String edited) {
		this.edited = edited;
	}


	public Timestamp getTimestamp() {
		return timestamp;
	}


	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}



}
