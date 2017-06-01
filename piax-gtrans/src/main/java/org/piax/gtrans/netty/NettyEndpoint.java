package org.piax.gtrans.netty;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.piax.common.ComparableKey;
import org.piax.common.Endpoint;
import org.piax.common.wrapper.DoubleKey;
import org.piax.common.wrapper.IntegerKey;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.netty.idtrans.PrimaryKey;

public interface NettyEndpoint extends Endpoint {
    public int getPort();
    public String getHost();
    public String getKeyString();

    static final Pattern SPLITTER = Pattern.compile("[^\\\\]:");

    static public List<String> parse(String input) {
        List<String> ret = new ArrayList<>(); 
        Matcher m = SPLITTER.matcher(input);
        while (m.find()) {
            ret.add(input.substring(0, m.end() - 1).replace("\\", ""));
            input = input.substring(m.end());
            m = SPLITTER.matcher(input);
        }
        ret.add(input);
        return ret;
    }

    // XXX very trivial key parser. 
    static public ComparableKey<?> parseKey(String input) {
        ComparableKey<?> key;
        try {
            if (input.equals("*")) { // wildcard.
                return null;
            }
            Number number = NumberFormat.getInstance().parse(input);
            int dot = input.indexOf('.'); // XXX just checking existence of '.'
            if (dot > 0) {
                key = new DoubleKey(number.doubleValue());
            }
            else {
                key= new IntegerKey(number.intValue());
            }
        } catch (ParseException e) {
            key = new StringKey(input);
        }
        return key;
    }
    
    public static void main(String args[]) {
        String in = "locator\\:123";
        ComparableKey<?> key;
        try {
            Number number = NumberFormat.getInstance().parse(in);
            int dot = in.indexOf('.');
            if (dot > 0) {
                key = new DoubleKey(number.doubleValue());
            }
            else {
                key= new IntegerKey(number.intValue());
            }
        } catch (ParseException e) {
            key = new StringKey(in);
        }
        System.out.println("" + key + key.getClass());
        
        for (String x: parse("tcp:localhost\\:1234:localhost:1234")) {
            System.out.println(x);
        }
    }

    public static NettyEndpoint newEndpoint(String spec) throws ProtocolUnsupportedException {
        // "id:pid1\:1:tcp:localhost:12367"
        // "tcp:localhost:12367"
        // "ssl:localhost:12367"
        // "udt:localhost:12367"
        List<String> specs = parse(spec);
        String s;
        if ((s = specs.get(0)) != null) {
            if (s.equals("id")) {
                return new PrimaryKey(parseKey(specs.get(1)), new NettyLocator(specs.get(2), specs.get(3), Integer.parseInt(specs.get(4))));
            }
            else {
                return new NettyLocator(specs.get(0), specs.get(1), Integer.parseInt(specs.get(2)));
            }
        }
        else {
            throw new ProtocolUnsupportedException("illegal use of constructor:");
        }
    }
}
