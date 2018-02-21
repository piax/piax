/*
 * CSFHookIf.java - A interface of Collective Store and
 * Forward in Ayame.
 * 
 * Copyright (c) 2017-2018 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 */
package org.piax.ayame.ov.rq.csf;

import org.piax.ayame.ov.rq.RQRequest;

public interface CSFHookIf<T> {
    /**
     * Returns proper name of the hook
     * 
     * @return name string
     */
    abstract String getName();

    /**
     * Make a decision on store/send/merge the request
     * 
     * @param req
     *            request to be send/store/merge
     * @param isRoot
     *            whether request is root or not
     * @return true if the method handled the request and caller should send this
     *         request, false if the request is not handled and caller should send
     *         the request by itself
     */
    abstract boolean storeOrForward(RQRequest<T> req, boolean isRoot);

    /**
     * Stop hook
     */
    abstract void fin();
}
