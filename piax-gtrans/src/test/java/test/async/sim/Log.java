package test.async.sim;

import java.io.ByteArrayInputStream;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log extends StreamHandler {

    public static boolean verbose = false;

    class LogFormatter extends Formatter {
        @Override
        public String format(LogRecord record) {
            return record.getMessage() + "\n";
        }
    }
    
    static public void init() {
        String conf = "handlers=test.async.sim.Log\n.level=ALL";
        try {
            LogManager.getLogManager().readConfiguration(new ByteArrayInputStream(conf.getBytes("utf-8")));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Log() {
        setOutputStream(System.out);
        setFormatter(new LogFormatter());
    }

    @Override
    public boolean isLoggable(LogRecord record) {
        // print only async related logs
        return (record.getLoggerName().startsWith("org.piax.gtrans.ov.async") ||
                record.getLoggerName().startsWith("org.piax.gtrans.async")) &&
                (record.getLevel() == Level.FINE ||
                (verbose && (record.getLevel() == Level.FINEST)));
    }

        @Override
    public void close() {
        // System.out is not closed.
    }

    @Override
    public void publish(LogRecord record) {
        super.publish(record);
        super.flush();
    }
    
    public static void main(String args[]) {
        Log.init();
        Logger logger = LoggerFactory.getLogger("org.piax.gtrans.async.LogTest");
        Log.verbose=true;
        logger.debug("debug");
        logger.trace("trace");
        System.out.println("---");
        Log.verbose=false;
        logger.debug("debug");
        logger.trace("trace");
    }
    
}