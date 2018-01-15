/*
 * CalleeId.java - A reference for a remote object.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Id.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;

import java.io.Serializable;

public class CalleeId implements Serializable {
    private static final long serialVersionUID = 1L;
    
    final protected ObjectId targetId;
    
    public ObjectId getTargetId() {
        return targetId;
    }
    
    /**
     * ピアの場所を示す
     */
    final private Endpoint peerRef;
    
    public Endpoint getPeerRef() {
        return peerRef;
    }
    
    /**
     * オブジェクトの名前
     * なければnull
     */
    final private String name;
    
    public String getName() {
        return name;
    }
    
    public CalleeId(ObjectId targetId, Endpoint peerRef, String name) {
        this.targetId = targetId;
        this.peerRef = peerRef;
        this.name = name;
    }
    
    @Override
    public int hashCode() {
        /*
         * 名前は、オブジェクトの同定には無関係なので
         * 比較の対象には含めない。
         */
        return targetId.hashCode()+peerRef.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CalleeId)) return false;
        CalleeId ref = (CalleeId)obj;
        if (!targetId.equals(ref.targetId)
                || !peerRef.equals(ref.peerRef)) {
            return false;
        }
        /*
         * 名前は、オブジェクトの同定には無関係なので
         * 比較の対象には含めない。
         */
        return true; 
    }
}
