package br.com.twitterSearch.mongo;

import java.util.Properties;

import org.bson.Document;

import br.com.twitterSearch.log.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Class that control MongoDB, creating database, collections and performing CRUD operations.
 * @author psilveira
 *
 */
public class MongoDB {

	private MongoDatabase db;
	private Properties config;
	private MongoClient mongo;
	private Logger logger;
	
	private static final String TWEET_COLLECTION = "tweet";
	private static final String TWITTER_USERS = "users";
	
	public MongoDB(Properties configFile, Logger logger) {
		this.config = configFile;
		this.logger = logger;
		initDB();
	}
	
	/**
	 * Initialize database using properties configuration.
	 */
	public void initDB() {
        logger.info("Connecting to Mongo DB..");
        
        mongo = new MongoClient(this.config.getProperty("MONGO_HOSTNAME"));
        db = mongo.getDatabase(this.config.getProperty("MONGO_DBNAME"));
        
        clearCollections();
	}
	
	public void clearCollections() {
		logger.info("Removing collections...");
		db.getCollection(TWEET_COLLECTION).drop();
		db.getCollection(TWITTER_USERS).drop();
	}
	
	/**
	 * Insert tweets to tweet collections.
	 * @param tweet Document that contains all data extracted from Twitter API
	 */
	public void insertTweet(Document tweet) {
		MongoCollection<Document> collection = db.getCollection(TWEET_COLLECTION);
		
		if (collection.find(new BasicDBObject("tweetId", tweet.get("tweetId"))).first() == null) {
			collection.insertOne(tweet);
		} else {
			collection.replaceOne(new BasicDBObject("tweetId", tweet.get("tweetId")), tweet);
		}
	}
	
	/**
	 * Insert twitter users to users collection.
	 * @param userId Twitter User ID
	 * @param username Twitter Username
	 * @param followers Total of followers
	 */
	public void insertUser(Object userId, Object username, Object followers) {
		MongoCollection<Document> collection = db.getCollection(TWITTER_USERS);
		
		Document doc = new Document();
		doc.put("userId", userId);
		doc.put("username", username);
		doc.put("followers", followers);
		if (collection.find(new BasicDBObject("userId", userId)).first() == null) {
			collection.insertOne(doc);
		} else {
			collection.replaceOne(new BasicDBObject("userId", userId), doc);
		}
	}

	/**
	 * Check if user already exists on users' collections
	 * @param userId Twitter User ID
	 * @return true if exists, false if not exists
	 */
	public boolean checkIfUserExists(Object userId) {
		MongoCollection<Document> collection = db.getCollection(TWITTER_USERS);
		
		if (collection.find(new BasicDBObject("userId", userId)).first() == null){
			return false;
		} else {
			return true;
		}
	}
}
