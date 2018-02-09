package org.piax.ayame.ov.rq.csf;

import org.piax.ayame.ov.rq.RQRequest;

public interface CSFHookIf<T> {
    abstract String getName();
    
    abstract boolean storeOrForward(RQRequest<T> req, boolean isRoot);
    
    abstract void fin();
}
