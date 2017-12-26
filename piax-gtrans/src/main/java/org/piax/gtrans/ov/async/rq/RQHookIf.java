package org.piax.gtrans.ov.async.rq;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.piax.gtrans.ov.async.rq.RQRequest;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;

public interface RQHookIf<T> {
    
    abstract boolean hook(RQRequest<T> req);
    
    abstract void fin();
    
    abstract void addHistory(RQRequest<T> req);
    
    abstract CompletableFuture<List<DKRangeRValue<T>>> executeLocal(RQRequest<T> req);
    
}
