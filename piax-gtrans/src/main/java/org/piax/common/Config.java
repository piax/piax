package org.piax.common;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Properties;

import org.piax.gtrans.async.Option;

public class Config {
    
    public static final String PROPERTY_FILE="piax.properties";

    static public void load(Properties properties) {
        try {
            for (Object k : properties.keySet()) {
                String key = (String) k;
                String value = properties.getProperty(key);
                int last = key.lastIndexOf('.');

                if (last < 0) {
                    System.err.println("Parse error: each property should have '<class name>.<option name> [= value]'");
                    System.exit(1);
                }
                String clazz = key.substring(0, last);
                Class<?> clz = Class.forName(clazz);

                String field = key.substring(last + 1);
                Field f = clz.getField(field);
                Option<?> o = (Option<?>) f.get(null);
                ArrayList<String> arg = new ArrayList<>();
                arg.add(value);
                o.parse(arg);
            }
        } catch (Exception e) {
            System.err.println(e); //例外処理
            System.exit(1);
        }
    }
    
    static public void load(String file) {
        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(file);
            properties.load(inputStream);
            load(properties);
            inputStream.close();
        } catch (Exception e) {
            System.err.println(e);  //例外処理
            System.exit(1);
        }
    }
    
    static public void load() {
        load(PROPERTY_FILE);
    }
    
    public static void main(String[] args) {
        
    }
}

