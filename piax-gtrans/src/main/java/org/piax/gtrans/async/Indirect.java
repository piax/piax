package org.piax.gtrans.async;

/**
 * a class to define a local variable that is modified in lambda expressions.
 *  
 * @param <U> type of the local variable
 */
public class Indirect<U> {
    public U val;
    public Indirect(U val) {
        this.val = val;
    }
    public Indirect() {
        // empty
    }
    @Override
    public String toString() {
        return val == null ? "null" : val.toString();
    }
}
