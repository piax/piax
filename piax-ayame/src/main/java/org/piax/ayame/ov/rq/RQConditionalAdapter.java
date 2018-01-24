package org.piax.ayame.ov.rq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.piax.ayame.FTEntry;
import org.piax.ayame.LocalNode;
import org.piax.ayame.ov.ddll.DdllKeyRange;
import org.piax.common.DdllKey;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * an adapter for conditional query
 *
 * @param <T> the type of return value
 * @param <U> the type of aggregated value
 */
public abstract class RQConditionalAdapter<T, U> extends RQAdapter<T> {
    private static final Logger logger = LoggerFactory.getLogger(RQConditionalAdapter.class);
    protected U current;
    public RQConditionalAdapter(Consumer<RemoteValue<T>> resultsReceiver) {
        super(resultsReceiver);
    }
    /* [条件付き範囲検索]
     * E = {} // entries to use
     * Q = クエリ範囲の集合
     * while (Q != empty) {
     *   Qから要素を1つ取り出し(削除)，qとする．
     *   // すべてのFTEntryで，qをカバーするものがあれば
     *   boolean found = false;
     *   foreach (FTEntry e) {
     *     if (provider.match(q, e)) { // eにマッチする
     *       eをEに加える
     *       qからeの範囲を削除し，残った範囲をQに追加する．
     *       found = true;
     *       break
     *     }
     *   }
     *   if (!found) {
     *       addRemoteValue(q, null)
     *   }
     * }
     * Eの各エントリに対してRQRequestを送信する．
     */
    //@Override
//    public List<RQRange> preprocess0(List<RQRange> queryRanges,
//            List<FTEntry> ftents, List<DKRangeRValue<T>> notForChildren) {
//        Class<? extends RQAdapter<T>> clazz = this.getClazz();
//        List<RQRange> rcopy = new ArrayList<>(queryRanges);
//        List<RQRange> forChildren = new ArrayList<>();
//        outer: while (!rcopy.isEmpty()) {
//            System.out.println("pre#rcopy=" + rcopy);
//            RQRange queryRange = rcopy.remove(0);
//            for (FTEntry ent: ftents) {
//                U val = (U) ent.getLocalCollectedData(clazz);
//                DdllKeyRange range = ent.getRange();
//                System.out.println("pre#qr=" + queryRange + ", range=" + range);
//                if (val != null && range != null
//                        && queryRange.intersects(range)
//                        && match(range, val)) {
//                    // entの範囲にクエリ対象が存在すると判定
//                    RQRange rv = new RQRange(null, range).assignSubId(queryRange);
//                    forChildren.add(rv);
//                    System.out.println("pre#add rv=" + rv);
//                    List<RQRange> remains = queryRange.retainRanges(range.from, range.to);
//                    System.out.println("pre#remains=" + remains);
//                    remains.stream().forEach(r -> {
//                        r.assignSubId(queryRange);
//                        rcopy.add(r);
//                    });
//                    continue outer;
//                }
//            }
//            // entの範囲にはクエリ対象が存在しないと判定
//            DKRangeRValue<T> rv = new DKRangeRValue<>(
//                    new RemoteValue<T>(null, (T)null), queryRange);
//            notForChildren.add(rv);
//        }
//        return forChildren;
//    }
    
    /**
     * Q = 検索領域の集合
     * M = matchするセグメントの集合
     * N = matchしないセグメントの集合
     * M' = {}  // 最終的にmatchするセグメントの集合
     * while M is not empty
     *   take m from M
     *   for (n in N)
     *     if (m と n が共通部分がある)
     *       m = m - n
     *       if (mが2つに分割)
     *         1つをMに追加し，残りをmとする．
     *       break if (m == empty);
     *   if (m is not empty)
     *     M’ = M' + m
     * (ここで，M'は最適化されたmatchするセグメントの集合となっている)
     * R = {} // q に含まれる，matchするセグメント
     * while Q is not emptyZZ
     *   take q from Q
     *   for (r in M')
     *     c = q ∩ r
     *     if (c is not empty) // 共通部分がある
     *       q -= c
     *       if (qが複数の区間に分割)
     *         1つをQに追加し，残りをqとする
     *       R += c
     *       break if (q == empty)
     * (Rがmatchする部分範囲)
     */
    @Override
    public List<RQRange> preprocess(List<RQRange> queryRanges,
            List<FTEntry> ftents, List<DKRangeRValue<T>> notForChildren) {
        // "refined" contains all FTEntry that matches the condition.
        // XXX: refinedを，カバーする範囲の小さい順にソートするべき
        List<Range<DdllKey>> refined = getMatchedRanges(ftents);
        logger.trace("refined={}", refined);

        // unmatched = target \ refined
        // matched = target ∧ refined
        List<RQRange> target = new ArrayList<>(queryRanges);
        List<RQRange> matched = new ArrayList<>();
        List<RQRange> unmatched = diffRanges(target, refined, matched, true);
        logger.trace("matched={}", matched);
        logger.trace("unmatched={}", unmatched);

        // unmatched に含まれる範囲は null を埋める
        for (Range<DdllKey> r: unmatched) {
            RemoteValue<T> val = new RemoteValue<>(null, (T)null);
            DKRangeRValue<T> rv = new DKRangeRValue<>(val, r);
            notForChildren.add(rv);
        }
        return matched;
    }

    /**
     * 経路表エントリから，matcherがマッチする範囲だけを抽出して返す．
     * マッチしない範囲は削る．
     * 
     * @param list of FTEntry
     * @return マッチする範囲のリスト
     */
    private List<Range<DdllKey>> getMatchedRanges(List<FTEntry> ftelist) {
        Class<? extends RQAdapter<T>> clazz = this.getClazz();
        Map<Boolean, List<FTEntry>> map = ftelist.stream()
                .collect(Collectors.groupingBy(fte -> {
                    @SuppressWarnings("unchecked")
                    U val = (U) fte.getLocalCollectedData(clazz);
                    // まだ最初の集約が終わっていない場合(val==null)，matchとする．
                    return (val == null || match(fte.getRange(), val));
                }));
        List<Range<DdllKey>> matched = (map.containsKey(true) ?
                    map.get(true).stream().map(ent -> 
                        ent.getRange()).collect(Collectors.toList()) : null);
        List<Range<DdllKey>> unmatched = (map.containsKey(false) ?
                        map.get(false).stream().map(ent -> 
                        ent.getRange()).collect(Collectors.toList()) : null);
        List<Range<DdllKey>> refined = diffRanges(matched, unmatched,
                null, true);
        return refined;
    }

    /**
     * compute (x \ y) and (x ∩ y).
     * note that this method destroys x.
     * 
     * @param x
     * @param y
     * @param intersections
     * @param isPartialAllowed true if elements in y that partially matches x
     *        are allowed.
     * @return x \ y
     */
    @SuppressWarnings("unchecked")
    private static <T extends Range<DdllKey>> List<T> diffRanges(
            List<T> x, List<? extends Range<DdllKey>> y, List<T> intersections,
            boolean isPartialAllowed) {
        /*
         * M' = {}  // 最終的にmatchするセグメントの集合
         * while M is not empty
         *   take m from M
         *   for (n in N)
         *     if (m と n が共通部分がある)
         *       m = m - n
         *       if (mが2つに分割)
         *         1つをMに追加し，残りをmとする．
         *       break if (m == empty);
         *   if (m is not empty)
         *    M’ = M' + m
         */
        List<T> unsubtracted = new ArrayList<>();
        while (!x.isEmpty()) {
            T q = x.remove(0);
            inner: for (Range<DdllKey> r: y) {
                List<T> retain;
                if (!isPartialAllowed) {
                    if (!q.contains(r)) {
                        continue;
                    }
                    // retain = q - r
                    retain = q.retain(r, null);
                    if (intersections != null) {
                        intersections.add((T) q.newRange(r));
                    }
                } else {
                    // intersect = q ∩ r
                    // retain = q - r
                    List<T> intersect = new ArrayList<>();
                    retain = q.retain(r, intersect);
                    if (intersections != null) {
                        intersections.addAll(intersect);
                    }
                }
                switch (retain.size()) {
                case 2:
                    x.add(retain.get(1));
                    q = retain.get(0);
                    break;
                case 1:
                    q = retain.get(0);
                    break;
                case 0:
                    q = null;
                    break inner;
                default:
                    throw new Error("unexpected retain.size()");
                }
            }
            if (q != null) {
                unsubtracted.add(q);
            }
        }
        return unsubtracted;
    }

    public U getCollectedData(LocalNode localNode) {
        throw new Error("you must override getCollectedData");
    }

    @Override
    public Object reduceCollectedData(List<?> value) {
        @SuppressWarnings("unchecked")
        List<U> vals = (List<U>)value;
        Object reduced = vals.stream()
                .reduce((a, b) -> reduce(a, b))
                .orElse(null);
        return reduced;
    }
    
    @Override
    public void handleResult(RemoteValue<T> result) {
        if (result != null && result.getValue() == null) {
            return; // ignore
        }
        super.handleResult(result);
    }

    public abstract U reduce(U a, U b);

    /**
     * 集約値に基づいて，クエリ対象が存在する可能性があるならばtrueを，
     * そうでなければfalseを返す．
     * 
     * @param range      あるFTEntryに対応する範囲
     * @param val        rangeに対応する集約値
     * @return true if there is a potentially match according to val.
     */
    public abstract boolean match(DdllKeyRange range, U val);
}