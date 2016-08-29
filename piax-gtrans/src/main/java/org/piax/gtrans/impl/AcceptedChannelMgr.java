/*
 * AcceptedChannelMgr.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: AcceptedChannelMgr.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;

/**
 * acceptしたChannelオブジェクトを、そのchannelを生成したピアのPeerIdとそのchannelのchannelNoを
 * 使って管理するためのクラス
 * 
 * 
 */
class AcceptedChannelMgr<E extends Endpoint> {
    
    static class Key {
        final PeerId creator;
        final int channelNo;
        Key(PeerId creator, int channelNo) {
            this.creator = creator;
            this.channelNo = channelNo;
        }
        
        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || !(o instanceof Key)) return false;
            if (hashCode() != o.hashCode()) return false;
            Key k = (Key) o;
            return creator.equals(k.creator) && channelNo == k.channelNo;
        }

        @Override
        public int hashCode() {
            int chash = (creator == null) ? 0 : creator.hashCode();
            return chash + channelNo;
        }
    }
    
    private final ConcurrentMap<Key, ChannelImpl<E>> channelMap = 
            new ConcurrentHashMap<Key, ChannelImpl<E>>();

    AcceptedChannelMgr() {
    }

    ChannelImpl<E> getChannel(PeerId creator, int channelNo) {
        return channelMap.get(new Key(creator, channelNo));
    }
    
    void putChannel(PeerId creator, int channelNo, ChannelImpl<E> ch) {
        channelMap.put(new Key(creator, channelNo), ch);
    }
    
    boolean removeChannel(ChannelImpl<E> ch) {
        // remove(key, value) メソッドを使っているのは、スレッドセーフに消去するため
        return channelMap.remove(new Key((PeerId) ch.creator, ch.getChannelNo()), ch);
    }
}
