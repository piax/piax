/*
 * ServiceInfo.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: ServiceInfo.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.tsd;

import java.io.Serializable;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;

/**
 * 
 */
class ServiceInfo<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final T info;
    protected final PeerId peerId;
    protected final ObjectId objId;

    ServiceInfo(T info, PeerId peerId, ObjectId objId) {
        this.info = info;
        this.peerId = peerId;
        this.objId = objId;
    }
    
//    public T getInfo() {
//        return info;
//    }
//
//    public ObjectId getObjectId() {
//        return objId;
//    }
    
    @Override
    public int hashCode() {
        return peerId.hashCode() ^ objId.hashCode() ^ info.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || !(obj instanceof ServiceInfo))
            return false;
        @SuppressWarnings("unchecked")
        ServiceInfo<T> _obj = (ServiceInfo<T>) obj;
        boolean i = (info == null) ? (_obj.info == null) :
            info.equals(_obj.info);
        if (!i) return false;
        boolean p = (peerId == null) ? (_obj.peerId == null) :
            peerId.equals(_obj.peerId);
        if (!p) return false;
        boolean o = (objId == null) ? (_obj.objId == null) :
            objId.equals(_obj.objId);
        return o;
    }
    
    @Override
    public String toString() {
        return info + "@" + peerId + "#" + objId;
    }
}
