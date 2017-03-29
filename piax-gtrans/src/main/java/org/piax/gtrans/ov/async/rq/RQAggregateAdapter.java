package org.piax.gtrans.ov.async.rq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.async.FTEntry;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;
import org.piax.gtrans.ov.ring.rq.DdllKeyRange;

/**
 * an adapter for aggregate query
 *
 * @param <T> the type of aggregate value
 */
public abstract class RQAggregateAdapter<T> extends RQAdapter<T> {
    protected T current;
    public RQAggregateAdapter(Consumer<RemoteValue<T>> resultsReceiver) {
        super(resultsReceiver);
    }
    /*
     * [集約クエリ]
     * R = {} // remain
     * Q = クエリ範囲の集合
     * while (Q != empty) {
     *   Qから要素を1つ取り出し(削除)，qとする．
     *   boolean found = false;
     *   foreach (FTEntry e) {
     *     if (provider.match(q, e)) { // e.valで済ませられる
     *       addRemoteValue(e.range, provider.getValue(e))
     *       qからeの範囲を削除し，残った範囲をQに追加する．
     *       found = true;
     *       break
     *     }
     *   }
     *   if (!found) {
     *     qをRに加える
     *   }
     * }
     * Rの各範囲をサブクエリに分解してRQRequestを送信する．
     */ 
    @Override
    public List<RQRange> preprocess(List<RQRange> queryRanges,
            List<FTEntry> ftents, List<DKRangeRValue<T>> locallyResolved) {
        Class<? extends RQAdapter<T>> clazz = this.getClazz();
        List<RQRange> rcopy = new ArrayList<>(queryRanges);
        List<RQRange> remain = new ArrayList<>();
        outer: while (!rcopy.isEmpty()) {
            RQRange queryRange = rcopy.remove(0);
            for (FTEntry ent: ftents) {
                T val = ent.getCollectedData(clazz);
                if (val != null && match(queryRange, ent.range, val)) {
                    DKRangeRValue<T> rv = new DKRangeRValue<>(new RemoteValue<T>(null, val), ent.range);
                    locallyResolved.add(rv);
                    List<RQRange> remains = queryRange.retainRanges(ent.range.from, ent.range.to);
                    remains.stream().forEach(r -> {
                        r.assignSubId(queryRange);
                        rcopy.add(r);
                    });
                    continue outer;
                }
            }
            remain.add(queryRange);
        }
        return remain;
    }

    @Override
    public void handleResult(RemoteValue<T> result) {
        if (result == null) {
            // send reduced value
            resultsReceiver.accept(new RemoteValue<>(null, current));
            // and terminate
            resultsReceiver.accept(null);
        } else if (current == null) {
            current = result.getValue();
        } else {
            current = reduce(current, result.getValue());
        }
    }
    
    @Override
    public Object getCollectedData(LocalNode localNode) {
        CompletableFuture<T> future = get(null, localNode.key);
        assert future.isDone();
        Object o = future.getNow(null);
        return o;
    }

    @Override
    public Object reduceCollectedData(List<?> value) {
        List<T> vals = (List<T>)value;
        Object reduced = vals.stream()
                .reduce((a, b) -> reduce(a, b))
                .orElse(null);
        return reduced;
    }

    public abstract T reduce(T a, T b);

    public abstract boolean match(RQRange queryRange,
            DdllKeyRange range, T val);
}