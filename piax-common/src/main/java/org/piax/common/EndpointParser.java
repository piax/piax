package org.piax.common;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.piax.gtrans.UnavailableEndpointError;

public class EndpointParser {
    static Map<String, Object> map = new HashMap<>();
    static {
        EndpointParser.registerParser("-tcp", "org.piax.gtrans.raw.tcp.TcpLocator");
        EndpointParser.registerParser("-udp", "org.piax.gtrans.raw.udp.UdpLocator");
        EndpointParser.registerParser("-emu", "org.piax.gtrans.raw.emu.EmuLocator");
        EndpointParser.registerParser("id", "org.piax.gtrans.netty.idtrans.PrimaryKey");
        EndpointParser.registerParser("tcp", "org.piax.gtrans.netty.NettyLocator");
        EndpointParser.registerParser("udt", "org.piax.gtrans.netty.NettyLocator");
        EndpointParser.registerParser("ssl", "org.piax.gtrans.netty.NettyLocator");
    }
    
    public static void registerParser(String spec, EndpointParsable parser) {
        Class clazz = parser.getClass();
        Method method = null;
        try {
            method = clazz.getMethod("parse", new Class[]{ String.class });
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
        map.put(spec, method);
    }

    public static void registerParser(String spec, String nameOfParsable) {
        map.put(spec, nameOfParsable);
    }

    public static void unregisterParser(String spec) {
        map.remove(spec);
    }

    public static String getSpec(String input) {
        String specs[] = input.split(":", 2);
        return specs[0];
    }

    public static Endpoint parse(String input) {
        String type = getSpec(input);
        Object obj = map.get(type);
        if (obj == null) {
            throw new UnavailableEndpointError("Endpoint type '" + type + "' is not registered.");
        }
        Method method = null;
        if (obj instanceof String) {
            try {
                Class clazz = Class.forName((String)obj);
                //Method method = clazz.getMethod("parse", new Class[]{ String.class });
                //Endpoint ep = (Endpoint)method.invoke(new Object[]{ spec });
                method = clazz.getMethod("parse", new Class[]{ String.class });
                map.put(type, method);
            } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
                throw new UnavailableEndpointError(e);
            }
        }
        else {
            method = (Method)obj;
        }
        Endpoint ep = null;
        try {
            ep = (Endpoint)method.invoke(null, new Object[]{ input });
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new UnavailableEndpointError(e);
        }
        return ep;

    }
}
