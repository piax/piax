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
import org.piax.gtrans.netty.idtrans.PrimaryKey;


// should be merged to Endpoint in the future.
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

    static public NettyEndpoint parsePrimaryKey(String spec) {
        List<String> specs = NettyEndpoint.parse(spec);
        return new PrimaryKey(parseKey(specs.get(1)), new NettyLocator(NettyLocator.parseType(specs.get(2)), specs.get(3), Integer.parseInt(specs.get(4))));
    }

    static public NettyEndpoint parseLocator(String spec) {
        List<String> specs = NettyEndpoint.parse(spec);
        return new NettyLocator(NettyLocator.parseType(specs.get(0)), specs.get(1), Integer.parseInt(specs.get(2)));
    }
}
