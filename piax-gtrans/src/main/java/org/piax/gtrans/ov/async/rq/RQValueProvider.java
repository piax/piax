package org.piax.gtrans.ov.async.rq;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import org.piax.gtrans.ov.ddll.DdllKey;

public interface RQValueProvider<T> extends Serializable {
    CompletableFuture<T> get(DdllKey key);

    public static class SimpleValueProvider implements RQValueProvider<DdllKey> {
        @Override
        public CompletableFuture<DdllKey> get(DdllKey key) {
            return CompletableFuture.completedFuture(key);
        }
    }
}
