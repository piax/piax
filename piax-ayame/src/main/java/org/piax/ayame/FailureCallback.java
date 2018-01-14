package org.piax.ayame;

@FunctionalInterface
public interface FailureCallback {
    void run(EventException e);
}
