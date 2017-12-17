package org.piax.gtrans.ov.async.rq;

import org.piax.gtrans.ov.async.rq.RQRequest;

public interface RQHookIf<T> {
    
    abstract boolean hook(RQRequest<T> req);
    
    abstract void fin();
    
    abstract void addHistory(RQRequest<T> req);
    
    abstract RQRequest<T> removeReceivedMessage(RQRequest<T> req);
    
}
