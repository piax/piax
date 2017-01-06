package org.piax.gtrans.async;

@FunctionalInterface
public interface SuccessCallback {
    public void run(LocalNode n);
}