package org.piax.ayame.ov.rq;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.piax.ayame.ov.rq.DKRangeRValue;
import org.piax.ayame.ov.rq.RQRequest;

public interface RQHookIf<T> {
    
    abstract boolean hook(RQRequest<T> req);
    
    abstract void fin();
    
    abstract void addHistory(RQRequest<T> req);
    
    abstract CompletableFuture<List<DKRangeRValue<T>>> executeLocal(RQRequest<T> req, List<RQRange> ranges);
    
}
