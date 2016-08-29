/*
 * NodeObserver.java - Observing certain events occurred in a DDLL node.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: FutureValues.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ddll;

import java.util.Collection;
import java.util.List;

/**
 * an interface for observing certain events occurred in a DDLL node.
 */
public interface NodeObserver {
    /**
     * called when the right link is changed by receiving a SetR message.
     * 
     * @param prevRight     the previous right link
     * @param newRight      the new right link
     * @param payload       an Object passed with SetR message.
     * 
     * @see Node#setR(Link, int, Link, Link, LinkNum, int, Object)
     */
    void onRightNodeChange(Link prevRight, Link newRight, Object payload);

    /**
     * called when the payload given via
     * {@link Node#setR(Link, int, Link, Link, LinkNum, int, Object)}
     * is not passed to the specified remote node.
     * typically this happens when the SetR request fails. 
     * 
     * @param payload   the payload not has been sent
     */

    void payloadNotSent(Object payload);

    /**
     * called when node failure is detected by DDLL's node monitor.
     * if this method returns true, the DDLL link fixing procedure is executed
     * just after returning from this method.
     * when false is returned, it is application's responsibility to repair the
     * failed link.
     * 
     * @param failedLinks
     * @return true to execute DDLL's link fixing procedure
     */
    boolean onNodeFailure(Collection<Link> failedLinks);
    
    List<Link> suppplyLeftCandidatesForFix();
}
