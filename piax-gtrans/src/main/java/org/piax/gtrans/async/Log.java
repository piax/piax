package org.piax.gtrans.async;

import java.util.function.Supplier;

public class Log {
    public static boolean verbose;
    
    public static void verbose(Supplier<String> supplier) {
        if (verbose) {
            System.out.println(supplier.get());
        }
    }
}
