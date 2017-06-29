package org.piax.gtrans.ov.async.suzaku;

import java.util.List;

import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.ReplyEvent;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.FTEntry;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.FTEntrySet;

public abstract class SuzakuEvent {
    public static class GetFTAllEvent
        extends RequestEvent<GetFTAllEvent, GetFTAllReplyEvent> {
        public GetFTAllEvent(Node receiver) {
            super(receiver);
        }
        @Override
        public void run() {
            LocalNode r = (LocalNode)receiver;
            FTEntry[][] ents = SuzakuStrategy.getSuzakuStrategy(r).getFingerTable();
            r.post(new GetFTAllReplyEvent(this, ents));
        }
    }

    public static class GetFTAllReplyEvent
        extends ReplyEvent<GetFTAllEvent, GetFTAllReplyEvent> {
        FTEntry[][] ents;
        public GetFTAllReplyEvent(GetFTAllEvent req, FTEntry[][] ents) {
            super(req);
            this.ents = ents;
        }
    }

    public static class GetFTEntEvent
        extends RequestEvent<GetFTEntEvent, GetFTEntReplyEvent> {
        final boolean isBackward;
        final int x;
        final int y;
        final int k;
        final FTEntrySet passive1;
        final FTEntrySet passive2;
        public GetFTEntEvent(Node receiver, boolean isBackward, int x, int y, int k,
                FTEntrySet passive1, FTEntrySet passive2) { 
            super(receiver);
            this.isBackward = isBackward;
            this.x = x;
            this.y = y;
            this.k = k;
            this.passive1 = passive1;
            this.passive2 = passive2;
        }
        @Override
        public void run() {
            LocalNode r = (LocalNode)receiver;
            SuzakuStrategy s = SuzakuStrategy.getSuzakuStrategy(r);
            FTEntrySet ent = s.getFingers(isBackward, x, y, k, passive1, passive2);
            r.post(this.composeReply(ent));
        }
        @Override
        public String toStringMessage() {
            return getClass().getSimpleName() + "(isBackward=" + isBackward
                    + ", x=" + x + ", y=" + y
                    + ", passive1=" + passive1 + ", passive2=" + passive2 + ")";
        }
        // to be overridden
        public GetFTEntReplyEvent composeReply(FTEntrySet ent) {
            return new GetFTEntReplyEvent(this, ent);
        }
    }

    public static class GetFTEntReplyEvent
        extends ReplyEvent<GetFTEntEvent, GetFTEntReplyEvent> {
        FTEntrySet ent;
        public GetFTEntReplyEvent(GetFTEntEvent req, FTEntrySet ent) {
            super(req);
            this.ent = ent;
        }
        @Override
        public String toStringMessage() {
            return getClass().getSimpleName() + "(" + ent + ")";
        }
    }

    /**
     * リモートノードにFinger Table Entryを教えるためのイベント．
     * 以下の場合に用いられる:
     * ・障害時
     * ・Lookup時にソース側ノードのFTEサイズが規定を下回る場合に補充
     */
    public static class FTEntUpdateEvent extends Event {
        public final FTEntry ent;
        public FTEntUpdateEvent(Node receiver, FTEntry ent) {
            super(receiver);
            this.ent = ent;
        }
        @Override
        public void run() {
            LocalNode r = (LocalNode)receiver;
            SuzakuStrategy.getSuzakuStrategy(r).updateFTEntry(this);
        }
        @Override
        public String toStringMessage() {
            return "FTEntUpdateEvent(ent=" + ent + ")";
        }
    }

    /**
     * リモートノードからFinger Table Entryを削除するイベント
     */
    public static class FTEntRemoveEvent extends Event {
        public final Node node; // to be removed
        public final List<Node> neighbors;
        public FTEntRemoveEvent(Node receiver, Node node, List<Node> neighbors) {
            super(receiver);
            this.node = node;
            this.neighbors = neighbors;
        }
        @Override
        public void run() {
            LocalNode r = (LocalNode)receiver;
            SuzakuStrategy.getSuzakuStrategy(r).removeFromFingerTable(node,
                    neighbors);
        }
        @Override
        public String toStringMessage() {
            return "FTEntRemoveEvent(node=" + node + ")";
        }
    }

    /**
     * リモートノードのReverse Pointerを削除するイベント
     */
    public static class RemoveReversePointerEvent extends Event {
        public RemoveReversePointerEvent(Node receiver) {
            super(receiver);
        }
        @Override
        public void run() {
            LocalNode r = (LocalNode)receiver;
            SuzakuStrategy.getSuzakuStrategy(r)
                .table.removeReversePointer(this.origin);
        }
        @Override
        public String toStringMessage() {
            return "RemoveReversePointerEvent(node=" + origin + ")";
        }
    }
}
