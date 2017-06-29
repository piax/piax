package org.piax.gtrans.async;


public class EventException extends Exception {
    public EventException(Exception cause) {
        super(cause);
    }
    public EventException(String s) {
        super(s);
    }
    public static class NetEventException extends EventException {
        public NetEventException(Throwable th) {
            super(th.getMessage());
        }
    }
    public static class TimeoutException extends EventException {
        public TimeoutException(String msg) {
            super(msg);
        }
        public TimeoutException() {
            this("Timeout");
        }
    }
    public static class AckTimeoutException extends TimeoutException {
        Node unresponsive;
        public AckTimeoutException(Node unresponsive) {
            super("Ack Timeout");
            this.unresponsive = unresponsive;
        }
        @Override
        public String toString() {
            return "AckTimeoutException(" + unresponsive + ")";
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
