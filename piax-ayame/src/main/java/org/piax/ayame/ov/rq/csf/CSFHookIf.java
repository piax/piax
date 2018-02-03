package org.piax.ayame.ov.rq.csf;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.piax.ayame.ov.rq.DKRangeRValue;
import org.piax.ayame.ov.rq.RQRange;
import org.piax.ayame.ov.rq.RQRequest;

public interface CSFHookIf<T> {
    
    abstract void setupRQ(RQRequest<T> req);
    
    abstract boolean hook(RQRequest<T> req);
    
    abstract void fin();
    
    abstract CompletableFuture<List<DKRangeRValue<T>>> executeLocal(RQRequest<T> req, List<RQRange> ranges);
}
