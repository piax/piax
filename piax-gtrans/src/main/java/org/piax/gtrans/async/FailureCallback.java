package org.piax.gtrans.async;

@FunctionalInterface
public interface FailureCallback {
    void run(EventException e);
}
