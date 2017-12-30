/*
 * DCLTranslator.java - A translator of DstCond.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: DCLTranslator.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.dcl;

import java.io.StringReader;
import java.util.List;

import org.piax.common.Destination;
import org.piax.common.Id;
import org.piax.common.dcl.parser.DCLParser;
import org.piax.common.dcl.parser.DCLParserVisitor;
import org.piax.common.dcl.parser.DCL_AndCondition;
import org.piax.common.dcl.parser.DCL_Between;
import org.piax.common.dcl.parser.DCL_BuiltinFunc;
import org.piax.common.dcl.parser.DCL_Destination;
import org.piax.common.dcl.parser.DCL_Enumeration;
import org.piax.common.dcl.parser.DCL_Floating;
import org.piax.common.dcl.parser.DCL_Integer;
import org.piax.common.dcl.parser.DCL_Interval;
import org.piax.common.dcl.parser.DCL_MinusNumber;
import org.piax.common.dcl.parser.DCL_Predicate;
import org.piax.common.dcl.parser.DCL_StartDCL;
import org.piax.common.dcl.parser.DCL_StartDestination;
import org.piax.common.dcl.parser.DCL_String;
import org.piax.common.dcl.parser.DCL_Var;
import org.piax.common.dcl.parser.ParseException;
import org.piax.common.dcl.parser.SimpleNode;
import org.piax.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that corresponds to a translator of DstCond.
 */
public class DCLTranslator implements DCLParserVisitor {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(DCLTranslator.class);
    
    public Destination parseDestination(String dcond) throws ParseException {
        StringReader reader = new StringReader(dcond);
        DCLParser parser = new DCLParser(reader);
        SimpleNode n = parser.StartDestination();
        DCLFactory exp;
        try {
            exp = (DCLFactory) n.jjtAccept(this, null);
        } catch (DCLParseException e) {
            throw new ParseException(e.getMessage());
        } catch (RuntimeException e) {
            throw new ParseException(e.getMessage());
        }
        return (Destination) exp.getDstCond();
    }
    
    @SuppressWarnings("unchecked")
    public DestinationCondition parseDCL(String dcond) throws ParseException {
        StringReader reader = new StringReader(dcond);
        DCLParser parser = new DCLParser(reader);
        SimpleNode n = parser.StartDCL();
        DCLFactory exp;
        try {
            exp = (DCLFactory) n.jjtAccept(this, null);
        } catch (DCLParseException e) {
            throw new ParseException(e.getMessage());
        } catch (RuntimeException e) {
            throw new ParseException(e.getMessage());
        }
        return new DestinationCondition((List<VarDestinationPair>) exp.getDstCond());
    }

    @Override
    public Object visit(SimpleNode node, Object data) {
        logger.error("unexpected visit call");
        return null;
    }

    public Object visit(DCL_StartDestination node, Object data) {
        logger.debug("visit DCL_Start1");
        return node.childrenAccept(this, new DestinationFactory());
    }

    public Object visit(DCL_Destination node, Object data) {
        logger.debug("visit DCL_DstPredicate");
        ((DestinationFactory) data).setOp((String) node.jjtGetValue());
        return node.childrenAccept(this, data);
    }

    public Object visit(DCL_StartDCL node, Object data) {
        logger.debug("visit DCL_Start2");
        return node.childrenAccept(this, new AndFactory());
    }

    public Object visit(DCL_AndCondition node, Object data) {
        logger.debug("visit DCL_AndCondition");
        return node.childrenAccept(this, data);
    }

    public Object visit(DCL_Predicate node, Object data) {
        logger.debug("visit DCL_Predicate");
        PredicateFactory pred = new PredicateFactory();
        pred.setOp((String) node.jjtGetValue());
        node.childrenAccept(this, pred);
        ((DCLFactory) data).add(pred.getDstCond());
        return data;
    }

    public Object visit(DCL_Var node, Object data) {
        logger.debug("visit DCL_Var");
        ((DCLFactory) data).add((String) node.jjtGetValue());
        return data;
    }

    public Object visit(DCL_Enumeration node, Object data) {
        logger.debug("visit DCL_Enumeration");
        EnumerationFactory list = new EnumerationFactory();
        node.childrenAccept(this, list);
        ((DCLFactory) data).add(list.getDstCond());
        return data;
    }

    public Object visit(DCL_Interval node, Object data) {
        logger.debug("visit DCL_Interval");
        IntervalFactory interval = new IntervalFactory((String) node.jjtGetValue());
        node.childrenAccept(this, interval);
        ((DCLFactory) data).add(interval.getDstCond());
        return data;
    }

    public Object visit(DCL_Between node, Object data) {
        logger.debug("visit DCL_Between");
        ((DCLFactory) data).add(null);
        return data;
    }

    public Object visit(DCL_BuiltinFunc node, Object data) {
        logger.debug("visit DCL_BuiltinFunc");
        BuiltinFuncFactory func = new BuiltinFuncFactory((String) node.jjtGetValue());
        node.childrenAccept(this, func);
        ((DCLFactory) data).add(func.getDstCond());
        return data;
    }

    public Object visit(DCL_MinusNumber node, Object data) {
        logger.debug("visit DCL_MinusNumber");
        return node.childrenAccept(this, data);
    }

    public Object visit(DCL_Integer node, Object data) {
        logger.debug("visit DCL_Integer");
        String num = (String) node.jjtGetValue();
        char post = num.charAt(num.length() - 1);
        int base, s, e;
        if (num.startsWith("0b")) {
            base = 2;
            s = 2;
        } else if (num.startsWith("0x")) {
            base = 16;
            s = 2;
        } else {
            base = 10;
            s = 0;
        }
        try {
            if (post == 'g' || post == 'G') {
                // ID
                e = num.length() - 1;
                byte[] idBytes;
                if (base == 16) {
                    idBytes = ByteUtil.hex2Bytes(num.substring(s, e));
                } else {
                    // TODO
                    // 16進数以外は一回longに変換するため精度の問題がある
                    long n = Long.parseLong(num.substring(s, e), base);
                    idBytes = ByteUtil.long2bytes(n);
                }
                Id id = new Id(idBytes);
                id.setHexString();
                ((DCLFactory) data).add(id);
            } else if (post == 'l' || post == 'L') {
                // Long
                e = num.length() - 1;
                long n = Long.parseLong(num.substring(s, e), base);
                if (node.jjtGetParent() instanceof DCL_MinusNumber) {
                    n = -n;
                }
                ((DCLFactory) data).add(n);
            } else {
                // Integer
                e = num.length();
                int n = Integer.parseInt(num.substring(s, e), base);
                if (node.jjtGetParent() instanceof DCL_MinusNumber) {
                    n = -n;
                }
                ((DCLFactory) data).add(n);
            }
        } catch (NumberFormatException e1) {
            throw new DCLParseException(e1);
        } catch (IllegalArgumentException e1) {
            throw new DCLParseException(e1);
        }
        return data;
    }

    public Object visit(DCL_Floating node, Object data) {
        logger.debug("visit DCL_Floating");
        // TODO doubleに決め付けている
        try {
            double num = Double.parseDouble((String) node.jjtGetValue());
            if (node.jjtGetParent() instanceof DCL_MinusNumber) {
                num = -num;
            }
            ((DCLFactory) data).add(num);
        } catch (NumberFormatException e) {
            throw new DCLParseException(e);
        }
        return data;
    }

    public Object visit(DCL_String node, Object data) {
        logger.debug("visit DCL_String");
        String str0 = (String) node.jjtGetValue();
        String str = str0.substring(1, str0.length() - 1);
        ((DCLFactory) data).add(str);
        return data;
    }
    
    // constant treated as 0-arity builtin function

//    public Object visit(DCL_Constant node, Object data) {
//        logger.debug("visit DCL_Constant");
//        // TODO 
//        throw new DCLParseException("constant not supported in this version");
////        String str = (String) node.jjtGetValue();
////        ((DCLFactory) data).add(str);
////        return data;
//    }
}
