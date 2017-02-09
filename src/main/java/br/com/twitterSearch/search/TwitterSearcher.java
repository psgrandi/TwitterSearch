package br.com.twitterSearch.search;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.bson.Document;

import twitter4j.HashtagEntity;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import br.com.twitterSearch.log.Logger;
import br.com.twitterSearch.mongo.MongoDB;

import com.mongodb.MongoException;

/**
 * Project to extract data from Twitter API and save to MongoDB.
 * @author psilveira
 *
 */
public class TwitterSearcher {

	private Properties config;
	private MongoDB mongoDB;
	private Logger logger;
	private final String DATE_FORMAT = "yyyyMMddHHmmss";
	private final String DATE_ONLY_FORMAT = "yyyyMMdd";
	private final String HOUR_ONLY_FORMAT = "HH";

	public TwitterSearcher(Properties config, Logger logger) throws MongoException, UnknownHostException {
		this.config = config;
		this.logger = logger;
		
		logger.info("Loading database...");
		this.mongoDB = new MongoDB(config, logger);
		logger.info("Database loaded!");
	}
	
	/**
	 * OAuth configuration to connect on Twitter API.
	 * @return ConfigurationBuilder
	 */
	public ConfigurationBuilder getTwitterConfiguration() {
		ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(Boolean.parseBoolean(config.getProperty("DebugEnabled")));
        cb.setJSONStoreEnabled(Boolean.parseBoolean(config.getProperty("JSONStoreEnabled")));
        cb.setOAuthConsumerKey(config.getProperty("OAuthConsumerKey"));
        cb.setOAuthConsumerSecret(config.getProperty("OAuthConsumerSecret"));
        cb.setOAuthAccessToken(config.getProperty("OAuthAccessToken"));
        cb.setOAuthAccessTokenSecret(config.getProperty("OAuthAccessTokenSecret"));
        
        return cb;
	}
	
	/**
	 * Method that connects to Twitter API and search tweets with specific hashtag. Each data found is send to MongoDB.
	 * 
	 * @param hashTag
	 * @throws TwitterException
	 */
	public void findTweetsByTag(String hashTag) throws TwitterException {
		Twitter twitter = new TwitterFactory(getTwitterConfiguration().build()).getInstance();
		
	    Query query = new Query(hashTag);
	    query.count(100);
	    
	    logger.info("Getting Tweets for hashtag " + hashTag + "...");
	    QueryResult result = twitter.search(query);
	    
	    logger.info("Formatting and saving result...");
	    for (Status tweet : result.getTweets()) {
	        Document doc = new Document();
	    	doc.put("tweetId",Long.toString(tweet.getId()));
	    	doc.put("userId", Long.toString(tweet.getUser().getId()));
	    	doc.put("username", tweet.getUser().getName());
	    	doc.put("followers", tweet.getUser().getFollowersCount());
	    	doc.put("language", tweet.getLang());
	    	doc.put("fulldate", new SimpleDateFormat(DATE_FORMAT).format(tweet.getCreatedAt()));
	    	doc.put("date", new SimpleDateFormat(DATE_ONLY_FORMAT).format(tweet.getCreatedAt()));
	    	doc.put("hour", new SimpleDateFormat(HOUR_ONLY_FORMAT).format(tweet.getCreatedAt()));
	    	doc.put("text", tweet.getText());

	    	List<Document> listTags = new ArrayList<Document>();
	    	for (HashtagEntity tagEntity : tweet.getHashtagEntities()) {
	    		Document tag = new Document("text", tagEntity.getText().toUpperCase());
	    		if (tag.getString("text").equalsIgnoreCase(hashTag.replace("#", ""))) {
	    			if (!listTags.contains(tag)){		    			
		    			listTags.add(tag);
	    			}
	    		}
	    	}
	    	
	    	doc.put("hashtags", listTags);	
	    	
	    	mongoDB.insertTweet(doc);
	    	mongoDB.insertUser(doc.get("userId"), doc.get("username"), doc.get("followers"));	    	
	    }	    	    
	    logger.info("Tweets successfully saved!");
	}
}
