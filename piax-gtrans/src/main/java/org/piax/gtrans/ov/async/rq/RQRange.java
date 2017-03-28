package org.piax.gtrans.ov.async.rq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.piax.common.subspace.CircularRange;
import org.piax.common.subspace.Range;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ring.rq.DdllKeyRange;

public class RQRange extends DdllKeyRange {
    private static final long serialVersionUID = 1L;
    public final static int MAXID = 100000;
    final Node delegate;
    public Integer[] ids;

    public RQRange(Node node, DdllKey from, DdllKey to) {
        this(node, from, to, null);
    }

    // a single point
    public RQRange(Node node, DdllKey key) {
        this(node, new Range<DdllKey>(key, true, key, true));
    }

    public RQRange(Node node, DdllKey from, DdllKey to, Integer[] ids) {
        super(from, true, to, false);
        this.delegate = node;
        this.ids = ids;
    }

    public RQRange(Node node, Range<DdllKey> subRange) {
        this(node, subRange, null);
    }

    public RQRange(Node node, Range<DdllKey> subRange, Integer[] ids) {
        super(subRange);
        this.delegate = node;
        this.ids = ids;
    }
    
    public Node getNode() {
        return delegate;
    }

    @Override
    public String toString() {
        return "[" + rangeString() + ":dele=" + delegate 
                + (ids != null ? ":ids=" + Arrays.toString(ids) : "")
                + "]";
    }

    @Override
    public RQRange[] split(DdllKey k) {
        CircularRange<DdllKey>[] s = super.split(k);
        // Java cannot cast CircularRange[] into SubRange[] so..
        RQRange[] ret = new RQRange[s.length];
        System.arraycopy(s, 0, ret, 0, s.length);
        return ret;
    }

    /**
     * このrangeを，entsの各エントリのkeyで指定された部分範囲に分割する．
     * 各部分範囲の担当ノードには，各エントリのvalueで指定されたNodeを割り当てる．
     * このrangeの左端の担当ノードは，
     * - 左端と一致するkeyがあれば，そのエントリを使用する．
     * - そうでなければ，nullとする．
     * 
     * @param ents
     * @return
     */
    public List<RQRange> split(NavigableMap<DdllKey, Node> ents) {
        List<RQRange> ranges = new ArrayList<RQRange>();
        Node aux = null;
        if (ents.containsKey(this.from)) {
            aux = ents.get(this.from);
        }
        RQRange r = this;
        for (Map.Entry<DdllKey, Node> ent : ents.entrySet()) {
            RQRange[] split = this.split(ent.getKey());
            if (split.length == 2) {
                ranges.add(new RQRange(aux, split[0]));
                aux = ent.getValue();
            }
            r = split[split.length - 1];
        }
        ranges.add(new RQRange(aux, r));
        return ranges;
    }

    /**
     * this rangeから [a, b) を削除した残りの範囲を返す．
     */
    public List<RQRange> retainRanges(DdllKey a, DdllKey b) {
        if (keyComp.compare(a, b) != 0 && keyComp.isOrdered(from, b, a)
                && keyComp.compare(from, a) != 0
                && keyComp.isOrdered(b, a, to)
                && keyComp.compare(to, b) != 0) {
            // (k-abe) not sure if this situation actually occurs
            // Range   [---------)
            //       -----b   a------
            // (keyComp.compare(from, a) != 0) がないと，
            // a = from && b = to の場合も真になってしまう．
            return Collections.singletonList(new RQRange(delegate, b, a));
        }
        List<RQRange> retains = new ArrayList<>();
        if (this.contains(a) && keyComp.compare(a, this.from) != 0) {
            // Range   [---------)
            //             a----..
            retains.add(new RQRange(delegate, from, a));
        }
        if (this.contains(b) && keyComp.compare(b, this.to) != 0) {
            // Range   [---------)
            //         ..-----b
            retains.add(new RQRange(delegate, b, to));
        }
        return retains;
    }

    /**
     * concatenate this range and another range.
     * delegate node is taken from this range or another range according
     * to auxRight.
     * 
     * @param another
     * @param auxRight
     * @return
     */
    public RQRange concatenate(RQRange another, boolean auxRight) {
        if (this.to.compareTo(another.from) != 0) {
            throw new IllegalArgumentException("not continuous: " + this
                    + " and " + another);
        }
        RQRange r = new RQRange((auxRight ? another.delegate : delegate), this.from,
                another.to);
        return r;
    }

    public RQRange assignId() {
        if (ids == null) {
            ids = new Integer[1];
            ids[0] = (int) (Math.random() * MAXID);
        }
        return this;
    }

    public void assignSubId(RQRange parent) {
        if (!isSameRange(parent)) {
            Integer[] ids = new Integer[parent.ids.length + 1];
            System.arraycopy(parent.ids, 0, ids, 0, parent.ids.length);
            ids[ids.length - 1] = (int) (Math.random() * MAXID);
            this.ids = ids;
        } else {
            this.ids = parent.ids; // no copy ok?
        }
    }
}
