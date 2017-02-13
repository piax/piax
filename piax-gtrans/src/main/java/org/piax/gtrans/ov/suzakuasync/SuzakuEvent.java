package org.piax.gtrans.ov.suzakuasync;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.ReplyEvent;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.Sim;
import org.piax.gtrans.ov.suzakuasync.SuzakuStrategy.FTEntrySet;

public abstract class SuzakuEvent {
    public static class GetFTAllEvent
        extends RequestEvent<GetFTAllEvent, GetFTAllReplyEvent> {
        public GetFTAllEvent(Node receiver,
                CompletableFuture<GetFTAllReplyEvent> future) {
            super(receiver, future);
        }
        @Override
        public void run() {
            LocalNode r = (LocalNode)receiver;
            FTEntry[][] ents = ((SuzakuStrategy)r.topStrategy).getFingerTable();
            r.post(new GetFTAllReplyEvent(this, ents));
        }
    }

    public static class GetFTAllReplyEvent
        extends ReplyEvent<GetFTAllEvent, GetFTAllReplyEvent> {
        FTEntry[][] ents;
        GetFTAllEvent req;
        public GetFTAllReplyEvent(GetFTAllEvent req, FTEntry[][] ents) {
            super(req);
            this.req = req;
            this.ents = ents;
        }
    }

    public static class GetFTEntEvent
        extends RequestEvent<GetFTEntEvent, GetFTEntReplyEvent> {
        final boolean isBackward;
        final int x;
        final int y;
        final int k;
        final FTEntrySet given;
        final FTEntry gift2;    // SUZAKU3
        public GetFTEntEvent(Node receiver, boolean isBackward, int x, int y, int k,
                FTEntrySet given, FTEntry gift2 /* for SUZAKU3 */, 
                CompletableFuture<GetFTEntReplyEvent> future) {
            super(receiver, future);
            this.isBackward = isBackward;
            this.x = x;
            this.y = y;
            this.k = k;
            this.given = given;
            this.gift2 = gift2;
        }
        @Override
        public void run() {
            if (false && !Sim.verbose) {
                System.out.println(receiver + " receives " + this + "\n"
                        + receiver.toStringDetail());
            }
            LocalNode r = (LocalNode)receiver;
            FTEntrySet ent = ((SuzakuStrategy)r.topStrategy)
                    .getFingers(isBackward, x, y, k, given, gift2);
            r.post(new GetFTEntReplyEvent(this, ent));
        }
        @Override
        public String toStringMessage() {
            return getClass().getSimpleName() + "(isBackward=" + isBackward
                    + ", x=" + x + ", y=" + y + ", given=" + given + ", gift2=" + gift2 + ")";
        }
    }

    public static class GetFTEntReplyEvent
        extends ReplyEvent<GetFTEntEvent, GetFTEntReplyEvent> {
        FTEntrySet ent;
        GetFTEntEvent req;
        public GetFTEntReplyEvent(GetFTEntEvent req, FTEntrySet ent) {
            super(req);
            this.req = req;
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
            ((SuzakuStrategy)r.topStrategy).updateFTEntry(this);
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
            ((SuzakuStrategy)r.topStrategy).removeFromFingerTable(node,
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
            ((SuzakuStrategy)r.topStrategy).table.removeReversePointer(this.origin);
        }
        @Override
        public String toStringMessage() {
            return "RemoveReversePointerEvent(node=" + origin + ")";
        }
    }
}
