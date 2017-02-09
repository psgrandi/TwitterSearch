package br.com.twitterSearch.main;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import twitter4j.TwitterException;
import br.com.twitterSearch.log.Logger;
import br.com.twitterSearch.search.TwitterSearcher;

public class TwitterSearchRun {
	
	static Logger logger = new Logger(true);
	static Properties config = new Properties();
	static InputStream input = null;
	static String filename = "config.properties";
	static List<String> tags2search = Arrays.asList("#brasil", "#brazil", "#brasil2016", "#brazil2016", 
			"#jogosolimpicos", "#olimpiadas", "#olimpiadas2016", "#olympics", "#rio2016", "#riodejaneiro");

	public static void main(String[] args) throws IOException, TwitterException {
		input = TwitterSearchRun.class.getClassLoader().getResourceAsStream(filename);
		
		if(input == null){
	        logger.info("Configuration file not found: " + filename);
		    throw new IOException("Configuration file not found: " + filename);
		}
		logger.info("Configuration file loaded successfully!");
		
		config.load(input);
		
		logger.info("Tweets extract started...");
		TwitterSearcher searcher = new TwitterSearcher(config, logger);
		for (String tag : tags2search) {
			searcher.findTweetsByTag(tag);
		}
		
		logger.info("Tweets extract finished!");
	}
	
}
