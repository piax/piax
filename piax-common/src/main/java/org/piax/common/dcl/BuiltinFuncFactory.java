/*
 * BuiltinFuncFactory.java - A Built-in-Function factory class of DCL.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: BuiltinFuncFactory.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.dcl;

import java.awt.geom.Point2D;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;

import org.piax.common.subspace.GeoCircle;
import org.piax.common.subspace.GeoEllipse;
import org.piax.common.subspace.GeoRectangle;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.Lower;
import org.piax.common.subspace.Near;
import org.piax.common.subspace.Upper;
import org.piax.common.wrapper.Keys;
import org.piax.common.wrapper.StringKey;
import org.piax.common.wrapper.WrappedComparableKey;

/**
 * A Built-in-Function of DstCond
 */
public class BuiltinFuncFactory implements DCLFactory {
    final String name;
    final List<Object> args = new ArrayList<Object>();

    static <K extends Comparable<?>> Lower<WrappedComparableKey<K>> newLower(
            boolean inclusive, K point, int k) {
        WrappedComparableKey<K> p = Keys.newWrappedKey(point);
        return new Lower<WrappedComparableKey<K>>(inclusive, p, k);
    }
    
    static <K extends Comparable<?>> Upper<WrappedComparableKey<K>> newUpper(
            boolean inclusive, K point, int k) {
        WrappedComparableKey<K> p = Keys.newWrappedKey(point);
        return new Upper<WrappedComparableKey<K>>(inclusive, p, k);
    }

    BuiltinFuncFactory(String name) {
        this.name = name;
    }

    @Override
    public void add(Object element) {
        args.add(element);
    }

    @Override
    public Object getDstCond() {
        if (name.equals("date")) {
            if (args.size() != 1) {
                throw new DCLParseException("date has one argument");
            }
            try {
//                DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
                return DateFormat.getDateInstance().parse((String) args.get(0));
            } catch (ClassCastException e) {
                throw new DCLParseException("date's argument is String");
            } catch (Exception e) {
                throw new DCLParseException(e);
            }
        }
        /*
         * TODO
         * 現時点で、point(x, y) に該当するのはLocationだけである。
         * 同様に、Regionのcontainsの対象はLocationであるが、ここでは、Point2D.Double
         * としてnewしている。Regionに汎用性をもたせた場合、point周辺の型について、見直す必要がある。
         */
        if (name.equals("point")) {
            if (args.size() != 2) {
                throw new DCLParseException("point has 2 arguments");
            }
            try {
                double x = ((Number) args.get(0)).doubleValue();
                double y = ((Number) args.get(1)).doubleValue();
                return new Point2D.Double(x, y);
            } catch (ClassCastException e) {
                throw new DCLParseException("point's arguments is double");
            }
        }
        if (name.equals("rect")) {
            try {
                if (args.size() == 4) {
                    double x = ((Number) args.get(0)).doubleValue();
                    double y = ((Number) args.get(1)).doubleValue();
                    double w = ((Number) args.get(2)).doubleValue();
                    double h = ((Number) args.get(3)).doubleValue();
                    return new GeoRectangle(x, y, w, h);
                } else if (args.size() == 3) {
                    Point2D p = (Point2D) args.get(0);
                    double w = ((Number) args.get(1)).doubleValue();
                    double h = ((Number) args.get(2)).doubleValue();
                    return new GeoRectangle(p, w, h);
                } else {
                    throw new DCLParseException("rect has 3 or 4 arguments");
                }
            } catch (ClassCastException e) {
                throw new DCLParseException("rect's arguments are Point2D or double");
            }
        }
        if (name.equals("ellipse")) {
            try {
                if (args.size() == 4) {
                    double x = ((Number) args.get(0)).doubleValue();
                    double y = ((Number) args.get(1)).doubleValue();
                    double w = ((Number) args.get(2)).doubleValue();
                    double h = ((Number) args.get(3)).doubleValue();
                    return new GeoEllipse(x, y, w, h);
                } else if (args.size() == 3) {
                    Point2D p = (Point2D) args.get(0);
                    double w = ((Number) args.get(1)).doubleValue();
                    double h = ((Number) args.get(2)).doubleValue();
                    return new GeoEllipse(p, w, h);
                } else {
                    throw new DCLParseException("ellipse has 3 or 4 arguments");
                }
            } catch (ClassCastException e) {
                throw new DCLParseException("ellipse's arguments are Point2D or double");
            }
        }
        if (name.equals("circle")) {
            try {
                if (args.size() == 3) {
                    double x = ((Number) args.get(0)).doubleValue();
                    double y = ((Number) args.get(1)).doubleValue();
                    double r = ((Number) args.get(2)).doubleValue();
                    return new GeoCircle(x, y, r);
                } else if (args.size() == 2) {
                    Point2D p = (Point2D) args.get(0);
                    double r = ((Number) args.get(1)).doubleValue();
                    return new GeoCircle(p, r);
                } else {
                    throw new DCLParseException("circle has 2 or 3 arguments");
                }
            } catch (ClassCastException e) {
                throw new DCLParseException("circle's arguments are Point2D or double");
            }
        }
        if (name.equals("near")) {
            try {
                if (args.size() == 3) {
                    double x = ((Number) args.get(0)).doubleValue();
                    double y = ((Number) args.get(1)).doubleValue();
                    int k = ((Number) args.get(2)).intValue();
                    return new Near(x, y, k);
                } else if (args.size() == 2) {
                    Point2D p = (Point2D) args.get(0);
                    int k = ((Number) args.get(1)).intValue();
                    return new Near(p, k);
                } else {
                    throw new DCLParseException("near has 2 or 3 arguments");
                }
            } catch (ClassCastException e) {
                throw new DCLParseException("near's arguments are Point2D or int");
            }
        }
        if (name.equals("maxLower")) {
            if (args.size() != 1) {
                throw new DCLParseException("maxLower has one argument");
            }
            try {
                return newLower(true, (Comparable<?>) args.get(0), 1);
            } catch (ClassCastException e) {
                throw new DCLParseException("maxLower's argument is Comparable");
            }
        }
        if (name.equals("lower")) {
            if (args.size() != 2) {
                throw new DCLParseException("lower has 2 arguments");
            }
            try {
                int k = ((Number) args.get(1)).intValue();
                return newLower(true, (Comparable<?>) args.get(0), k);
            } catch (ClassCastException e) {
                throw new DCLParseException("lower's arguments are Comparable or int");
            }
        }
        if (name.equals("lowerThan")) {
            if (args.size() != 2) {
                throw new DCLParseException("lowerThan has 2 arguments");
            }
            try {
                int k = ((Number) args.get(1)).intValue();
                return newLower(false, (Comparable<?>) args.get(0), k);
            } catch (ClassCastException e) {
                throw new DCLParseException("lowerThan's arguments are Comparable or int");
            }
        }
        if (name.equals("minUpper")) {
            if (args.size() != 1) {
                throw new DCLParseException("minUpper has one argument");
            }
            try {
                return newUpper(true, (Comparable<?>) args.get(0), 1);
            } catch (ClassCastException e) {
                throw new DCLParseException("minUpper's argument is Comparable");
            }
        }
        if (name.equals("upper")) {
            if (args.size() != 2) {
                throw new DCLParseException("upper has 2 arguments");
            }
            try {
                int k = ((Number) args.get(1)).intValue();
                return newUpper(true, (Comparable<?>) args.get(0), k);
            } catch (ClassCastException e) {
                throw new DCLParseException("upper's arguments are Comparable or int");
            }
        }
        if (name.equals("upperThan")) {
            if (args.size() != 2) {
                throw new DCLParseException("upperThan has 2 arguments");
            }
            try {
                int k = ((Number) args.get(1)).intValue();
                return newUpper(false, (Comparable<?>) args.get(0), k);
            } catch (ClassCastException e) {
                throw new DCLParseException("upperThan's arguments are Comparable or int");
            }
        }
        if (name.equals("prefix")) {
            if (args.size() != 1) {
                throw new DCLParseException("prefix has one argument");
            }
            try {
                String s = (String) args.get(0);
                StringKey start = new StringKey(s);
                // TODO think!
                int n = s.length() - 1;
                char c = (char) (s.charAt(n) + 1);
                StringKey end = new StringKey(s.substring(0, n) + c);
                return new KeyRange<StringKey>(start, true, end, false);
            } catch (ClassCastException e) {
                throw new DCLParseException("prefix's argument is String");
            }
        }
        
        // constant treated as 0-arity builtin function
        // この処理も含めて将来的に対処する
        throw new DCLParseException("func/constant \"" + name + "\" is not supported");
    }
}
