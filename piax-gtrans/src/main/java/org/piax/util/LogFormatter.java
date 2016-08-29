/*
 * LogFormatter.java - A log formatter SLF4J log-level.
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter {
	enum LogLevel {error, info, debug, warn, trace} 
	static public SimpleDateFormat sdf = new SimpleDateFormat("[yyyy/MM/dd HH:mm:ss:SSS z]");
	@Override
	public String format(LogRecord record) {
		String ret = sdf.format(new Date()) + " " +
				record.getSourceClassName() + "#" + record.getSourceMethodName() + " " +
				"[" + getLogLevel(record.getLevel()) + "] " + record.getMessage();
		Throwable thrown = record.getThrown();
		if (thrown != null) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			thrown.printStackTrace(pw);
			pw.close();
			ret += sw.toString();
		}
		return ret + "\n";
	}
	
	public static LogLevel getLogLevel(Level l) {
		LogLevel ret = LogLevel.trace;
		if (l.equals(Level.SEVERE)) {
			ret = LogLevel.error;
		}
		else if (l.equals(Level.WARNING)) {
			ret = LogLevel.warn;
		}
		else if (l.equals(Level.INFO)) {
			ret = LogLevel.info;
		}
		else if (l.equals(Level.FINE)) {
			ret = LogLevel.debug;
		}
		return ret;
	}
}
