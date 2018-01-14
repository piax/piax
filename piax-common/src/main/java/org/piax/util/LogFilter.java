/*
 * LogFilter.java - Good old SimpleLog-like log filter.
 * 
 * Copyright (c) 2015 PIAX development team 
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Channel.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.util;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFilter implements Filter {
	public enum LogLevel {error, warn, info, debug, trace};

	static public Level defaultLevel = getLevel("info");
	static public Map<String, Level> logLevelMap = new HashMap<String, Level>();
	
	public boolean isLoggable(LogRecord record) {
		LogManager lm = LogManager.getLogManager();
		String defaultLevelStr = lm.getProperty(".loglevel");
		
		if (defaultLevelStr != null) {
			defaultLevel = getLevel(defaultLevelStr);
		}
		String levelStr = lm.getProperty(record.getLoggerName() + ".loglevel");
			if (levelStr != null) {
			Level l = getLevel(levelStr);
			if (l != null) {
				if (l.intValue() <= record.getLevel().intValue()) {
						return true;
				}
			}
				return false;
		}
		else if (defaultLevel.intValue() <= record.getLevel().intValue()){
				return true;
		}
		return false;
	}
	
	public static Level getLevel(String l) {
		Level ret = defaultLevel;
		if (l.equals("error")) {
			ret = Level.SEVERE;
		} else if (l.equals("warn")) {
			ret = Level.WARNING;
		} else if (l.equals("info")) {
			ret = Level.INFO;
		} else if (l.equals("debug")) {
			ret = Level.FINE;
		} else if (l.equals("trace")) {
			ret = Level.FINEST;
		}
		return ret;
	}
	
	public static void main(String args[]) {
		Logger logger = LoggerFactory.getLogger(LogFilter.class);
		
		logger.debug("debug {}", "message");
		logger.info("info {}", "message");
		logger.warn("warn {}", "message");
		logger.error("error {}", "message");
		logger.trace("trace {}", "message");
	}
}
