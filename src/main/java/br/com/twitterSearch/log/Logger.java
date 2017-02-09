package br.com.twitterSearch.log;

public class Logger {
	
	private boolean LOGGER_FLAG;
	
	public Logger(boolean flag) {
		LOGGER_FLAG = flag;
	}
	
	public void info(String msg) {
		if (LOGGER_FLAG) {
			System.out.println("INFO - " + msg);
		}
	}
	
	public void error(String msg) {
		if (LOGGER_FLAG) {
			System.out.println("ERROR - " + msg);
			System.exit(1);
		}
	}
}
