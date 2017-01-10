package org.piax.gtrans.async;

import org.piax.gtrans.RPCException;

public class EventException extends Exception {
    public EventException(Exception cause) {
        super(cause);
    }
    public EventException(String s) {
        super(s);
    }
    public static class RPCEventException extends EventException {
        public RPCEventException(RPCException e) {
            super(e);
        }
    }
    public static class TimeoutException extends EventException {
        public TimeoutException() {
            super("timeout");
        }
    }
    public static class RetriableException extends EventException {
        public RetriableException() {
            super("retriable exception");
        }
        public RetriableException(String info) {
            super(info);
        }
    }
    public static class GraceStateException extends RetriableException {
        public GraceStateException() {
            super("received in grace state");
        }
    }
}
