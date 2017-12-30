package org.piax.common;

import java.util.HashMap;
import java.util.Map;

public class EndpointParser {
    static Map<String, EndpointParsable> map = new HashMap<>();
    
    public static void registerParser(String spec, EndpointParsable parser) {
        map.put(spec, parser);
    }
    
    public static void unregisterParser(String spec) {
        map.remove(spec);
    }
    
    public static String getSpec(String input) {
        String specs[] = input.split(":", 2);
        return specs[0];
    }
    
    public static Endpoint parse(String input) {
        /*if (input.startsWith("-")) {
            return PeerLocator.newLocator(input.substring(1)); // -tcp:localhost:12367
        }*/
        EndpointParsable parser = map.get(getSpec(input));
        return parser == null ? null : parser.parse(input);
    }
}
