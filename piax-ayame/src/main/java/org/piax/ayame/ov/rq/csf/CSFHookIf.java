package org.piax.ayame.ov.rq.csf;

import org.piax.ayame.ov.rq.RQRequest;

public interface CSFHookIf<T> {
    abstract String getName();
    
    abstract boolean setupRQ(RQRequest<T> req);
    
    abstract boolean storeOrForward(RQRequest<T> req, boolean isRoot);
    
    abstract long toDeadline(RQRequest<T> req);
    
    abstract void fin();
}
