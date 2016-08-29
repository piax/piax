/*
 * TransportIdPath.java - The path information of a TransportId.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: TransportIdPath.java 1189 2015-06-06 14:57:58Z teranisi $
 */

package org.piax.common;

import java.util.ArrayList;
import java.util.List;

import org.piax.gtrans.GTransConfigValues;

/**
 * 階層構造にあるTransportにおいて、上位層に向かう順序で最下位層から当該TransportのTransportId
 * を並べたListを扱うクラス。
 * <p>
 * 他のTransportIdPathと辞書的順序にならった比較が可能。
 * また、他のTransportIdPathとsuffixが一致するかどうかの判定を行うmatchesメソッドも持つ。
 * <p>
 * TransportIdPathはimmutableである。このため、メソッドの同期化は行っていない。
 */
public class TransportIdPath implements ComparableKey<TransportIdPath> {
    private static final long serialVersionUID = 1L;

    private final List<TransportId> path;
    private final int hash; // cache

    public TransportIdPath(byte[]... transIds) {
        path = new ArrayList<TransportId>();
        int h = 0;
        for (byte[] b : transIds) {
            TransportId id = new TransportId(b);
            path.add(id);
            h += id.hashCode();
        }
        hash = h;
    }

    public TransportIdPath(String... transIds) {
        path = new ArrayList<TransportId>();
        int h = 0;
        for (String str : transIds) {
            TransportId id = new TransportId(str);
            path.add(id);
            h += id.hashCode();
        }
        hash = h;
    }
    
    public TransportIdPath(TransportId... transIds) {
        path = new ArrayList<TransportId>();
        int h = 0;
        for (TransportId id : transIds) {
            path.add(id);
            h += id.hashCode();
        }
        hash = h;
    }
    
    public TransportIdPath(TransportIdPath lowerIdPath, TransportId transId) {
        if (transId == null)
            throw new IllegalArgumentException("transId should not be null");
        path = new ArrayList<TransportId>();
        int h = 0;
        if (lowerIdPath != null) {
            path.addAll(lowerIdPath.path);
            h = lowerIdPath.hashCode();
        }
        path.add(transId);
        h += transId.hashCode();
        hash = h;
    }

    public List<TransportId> getPath() {
        return new ArrayList<TransportId>(path);
    }
    
    /**
     * 指定されたTransportIdPathがこのTransportIdPathのsuffixとして一致するかどうかを判定する。
     * 
     * @param suffix TransportIdPath
     * @return 指定されたTransportIdPathがこのTransportIdPathのsuffixとして一致する場合にtrue
     */
    public boolean matches(TransportIdPath suffix) {
        int offset = path.size() - suffix.path.size();
        if (offset < 0) return false;
        for (TransportId id : suffix.path) {
            if (!id.equals(path.get(offset++))) return false;
        }
        return true;
    }
    
    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (hashCode() != o.hashCode())
            return false;
        if (getClass() != o.getClass())
            return false;
        TransportIdPath other = (TransportIdPath) o;
        if (path.size() != other.path.size())
            return false;
        for (int i = 0; i < path.size(); i++) {
            if (!path.get(i).equals(other.path.get(i))) return false;
        }
        return true;
    }

    /**
     * 辞書的順序に従い、引数で指定されたTransportIdPathと比較する。
     * 
     * @param o 比較対象のTransportIdPath
     * @return 辞書的順序として見た場合、このTransportIdPathが小さい場合-1、一致する場合 0、大きい場合1
     */
    @Override
    public int compareTo(TransportIdPath o) {
        for (int i = 0; i < Math.max(path.size(), o.path.size()); i++) {
            if (i == path.size()) return -1;
            if (i == o.path.size()) return 1;
            int c = path.get(i).compareTo(o.path.get(i));
            if (c != 0) return c;
        }
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (TransportId id : path) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(GTransConfigValues.ID_PATH_SEPARATOR);
            }
            sb.append(id);
        }
        return sb.toString();
    }
}
