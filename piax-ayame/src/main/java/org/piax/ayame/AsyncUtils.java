package org.piax.ayame;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.piax.ayame.EventException.RetriableException;

public class AsyncUtils {
    public static <T> CompletableFuture<T> rejectedFuture(Throwable exc) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        cf.completeExceptionally(exc);
        return cf;
    }
    
    @SuppressWarnings("unchecked")
    public static <T> CompletableFuture<T> thenCompose(
            CompletableFuture<T> cf,
            BiFunction<T, ? super Throwable, CompletableFuture<T>> func
            ) {
        return cf.handle((result, err) -> {
            if (err != null) {
                // forcibly convert exceptional cases to ordinal cases to use
                // CompletableFuture.thenCompose()
                return (T)err;
            }
            return result;
        }).thenCompose(result -> {
            Throwable err = null;
            if (result instanceof Throwable) {
                err = (Throwable)result;
                result = null;
            }
            return func.apply(result, err);
        });
    }

    public static <T> CompletableFuture<T> retryAsync(
            Function<Integer, CompletableFuture<T>> func,
            int count
            ) {
        assert count >= 1;
        return thenCompose(func.apply(count), (result, err) -> {
            if (err != null) {
                if (err instanceof RetriableException && count - 1 > 0) {
                    return retryAsync(func, count - 1);
                } else {
                    throw new CompletionException(err);
                }
            }
            return CompletableFuture.completedFuture(result);
        });
    }

    /*@Test
    public void testRetry() {
        AsyncUtils.<Integer>retryAsync(count -> {
            System.out.println("called " + count);
            CompletableFuture<Integer> cf = new CompletableFuture<>();
            if (count > 1) {
                cf.completeExceptionally(new RetriableException("hoge"));
            } else {
                cf.complete(100);
            }
            return cf;
        }, 5).whenComplete((result, err) -> {
            System.out.println("val=" + result + ", err=" + err);
        });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {}
    }*/
}
