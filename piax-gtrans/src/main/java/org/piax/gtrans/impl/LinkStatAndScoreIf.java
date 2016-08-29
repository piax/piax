/*
 * LinkStatAndScoreIf.java
 * 
 * Copyright (c) 2009-2015 National Institute of Information and 
 * Communications Technology
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: LinkStatAndScoreIf.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import org.piax.common.PeerLocator;

/**
 * linkの状態監視とスコアリングのためのinterface。
 * BaseTransportMgrで使われる。
 * 
 * 
 */
public interface LinkStatAndScoreIf {
    String evalFormat();
    Integer[] eval(PeerLocator src, PeerLocator dst);
}
