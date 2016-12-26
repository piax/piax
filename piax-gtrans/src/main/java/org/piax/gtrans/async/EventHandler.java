package org.piax.gtrans.async;

@FunctionalInterface
public interface EventHandler<T extends Event> {
    public void handle(T ev);
}
