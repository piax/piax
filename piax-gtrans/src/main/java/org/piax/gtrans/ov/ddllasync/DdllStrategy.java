package org.piax.gtrans.ov.ddllasync;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.async.FailureCallback;
import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.async.EventDispatcher;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.NetworkParams;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.Node.NodeMode;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.async.NodeStrategy;
import org.piax.gtrans.async.Option.EnumOption;
import org.piax.gtrans.async.Option.IntegerOption;
import org.piax.gtrans.async.Sim;
import org.piax.gtrans.async.SuccessCallback;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddllasync.DdllEvent.Ping;
import org.piax.gtrans.ov.ddllasync.DdllEvent.Pong;
import org.piax.gtrans.ov.ddllasync.DdllEvent.PropagateNeighbors;
import org.piax.gtrans.ov.ddllasync.DdllEvent.SetL;
import org.piax.gtrans.ov.ddllasync.DdllEvent.SetR;
import org.piax.gtrans.ov.ddllasync.DdllEvent.SetRAck;
import org.piax.gtrans.ov.ddllasync.DdllEvent.SetRNak;

public class DdllStrategy extends NodeStrategy {
    public static class DdllNodeFactory extends NodeFactory {
        @Override
        public LocalNode createNode(TransportId transId,
                ChannelTransport<?> trans, DdllKey key, int latency)
                        throws IOException, IdConflictException {
            return new LocalNode(transId, trans, key, new DdllStrategy(),
                    latency);
        }

        @Override
        public String name() {
            return "DDLL";
        }
    }

    public static enum DdllStatus {
        OUT, INS, DEL, IN
    };

    public static int JOIN_RETRY_DELAY = 2;

    // SETRNAKOPT: SetRNakメッセージにsuccessor, predecessor情報を入れる．
    // joinリトライ時にそれをそのまま使ってSetR送信．
    //public static boolean SETRNAKOPT = false;
    // SETRNAKOPT2: SetRNakメッセージにsuccessor, predecessor情報を入れる．
    // joinリトライ時，predecessorに変化がなければそのまま使ってSetR送信．
    // 変わっていればSetRを送っても無駄なので再検索する．
    //public static boolean SETRNAKOPT2 = false;
    public enum SetRNakMode {
        SETRNAK_NONE, SETRNAK_OPT1, SETRNAK_OPT2
    };

    public static EnumOption<SetRNakMode> setrnakmode = new EnumOption<>(
            SetRNakMode.class, SetRNakMode.SETRNAK_NONE, "-setrnak");
    // pinging is off by default
    public static IntegerOption pingPeriod =
            new IntegerOption(0, "-pingperiod");

    int lseq = 0, rseq = 0;

    DdllStatus status = DdllStatus.OUT;
    /** neighbor node set */
    public NeighborSet leftNbrs;

    public int joinTime = -1;
    private int joinMsgs = 0;

    private SuccessCallback leaveCallback;

    public static void load() {
    }

    public DdllStrategy() {
    }

    @Override
    public void setupNode(LocalNode node) {
        super.setupNode(node);
        this.leftNbrs = new NeighborSet(node);
    }

    @Override
    public String toStringDetail() {
        return "N" + n.key + "(succ=" + (n.succ != null ? n.succ.key : "null")
                + ", pred=" + (n.pred != null ? n.pred.key : "null")
                + ", status=" + status + ", lseq=" + lseq + ", rseq=" + rseq
                + ", nbrs=" + leftNbrs + ")";
    }

    public DdllStatus getStatus() {
        return status;
    }

    @Override
    public void initInitialNode() {
        n.succ = n;
        n.pred = n;
        status = DdllStatus.IN;
        schedulePing();
    }

    @Override
    public void joinAfterLookup(LookupDone l, SuccessCallback cb,
            FailureCallback eh) {
        join(l.pred, l.succ, cb, eh);
    }

    public void joinAfterLookup(LookupDone l, SuccessCallback job) {
        join(l.pred, l.succ, job, null);
    }

    public void join(Node pred, Node succ, SuccessCallback job,
            FailureCallback eh) {
        n.pred = pred;
        n.succ = succ;
        status = DdllStatus.INS;
        Event ev = new SetR(n.pred, n, n.succ, 0, job);
        n.post(ev, (exc) -> {
            System.out.println(n + ": join: SetRAck/Nak timeout. ");
            if (eh != null) eh.run(exc);
            // fix(ev.receiver); // recovery
        });
    }

    @Override
    public void leave(SuccessCallback callback) {
        leave(callback, null);
    }

    public void leave(SuccessCallback callback, SuccessCallback job) {
        System.out.println(n + ": leave start");
        status = DdllStatus.DEL;
        this.leaveCallback = callback;
        Event ev = new SetR(n.pred, n.succ, n, rseq + 1, job);
        n.post(ev, (exc) -> {
            System.out.println(n + ": leave: SetRAck/Nak timeout. retry.");
            fix(ev.receiver); // recovery
            leave(callback, job); // and retry!
        });
    }
    
    public void handleLookup(Lookup l) {
        if (isResponsible(l.key)) {
            n.post(new LookupDone(l, n, n.succ));
        } else {
            n.forward(n.succ, l);
        }
    }

    public void setr(SetR msg) {
        if (status != DdllStatus.IN || msg.rCur != n.succ) {
            if (status != DdllStatus.IN && status != DdllStatus.DEL) {
                EventDispatcher.addCounter("SetR:!IN");
                n.post(new SetRNak(msg, null, null)); // XXX:
            } else {
                EventDispatcher.addCounter("SetR:mismatch");
                // ME ----- U ----  ME.R
                if (Node.isIn(msg.rNew.key, n.key, n.succ.key)) {
                    n.post(new SetRNak(msg, n, n.succ));
                } else {
                    // ME ----- ME.R ----  U
                    n.post(new SetRNak(msg, n.succ, msg.rCur));
                }
            }
        } else {
            boolean forInsertion = msg.origin == msg.rNew;
            if (forInsertion) {
                leftNbrs.add(msg.rNew);
            } else {
                leftNbrs.removeNode(msg.rCur);
            }
            // compute a neighbor node set to send to the new right node 
            Set<Node> nset = leftNbrs.computeNSForRight(msg.rNew);
            if (forInsertion) {
                Set<Node> nset2 = leftNbrs.computeNSForRight(getSuccessor());
                n.post(new SetL(n.succ, msg.rNew, rseq + 1, nset2));
            } else {
                n.post(new SetL(msg.rNew, n, msg.rnewseq, nset));
            }
            leftNbrs.setPrevRightSet(msg.rNew, nset);
            n.post(new SetRAck(msg, rseq + 1, nset));
            n.setSucc(msg.rNew);
            rseq = msg.rnewseq;
            if (msg.successCallback != null) {
                msg.successCallback.run(n);
            }
        }
    }

    public void setrack(SetRAck msg, Set<Node> nbrs) {
        if (status == DdllStatus.INS) {
            joinMsgs += 3; // SetR, SetRAck and SetL
            status = DdllStatus.IN;
            rseq = msg.rnewnum;
            leftNbrs.set(nbrs);
            // nbrs does not contain the immediate left node
            leftNbrs.add(getPredecessor());
            System.out.println(n + ": INSERTED, vtime = " + msg.vtime
                    + ", latency=" + n.latency);
            msg.req.successCallback.run(n);
            //listener.nodeInserted();
            schedulePing();
        } else {
            status = DdllStatus.OUT;
            System.out.println(n + ": DELETED, vtime = " + msg.vtime
                    + ", latency=" + n.latency);
            if (leaveCallback != null) {
                leaveCallback.run(n);
            }
        }
    }

    public void setrnak(SetRNak msg) {
        if (status == DdllStatus.INS) {
            joinMsgs += 2; // SetR and SetRNak
            status = DdllStatus.OUT;
            // retry!
            LocalNode.verbose("receive SetRNak: join retry, pred=" + msg.pred
                    + ", succ=" + msg.succ);
            if (setrnakmode.value() == SetRNakMode.SETRNAK_OPT2) {
                // DDLL with optimization2
                if (msg.pred == n.pred) {
                    join(msg.pred, msg.succ, msg.req.successCallback, msg.req.failureCallback);
                } else {
                    n.joinUsingIntroducer(msg.pred, msg.req.successCallback, msg.req.failureCallback);
                }
            } else if (setrnakmode.value() == SetRNakMode.SETRNAK_OPT1) {
                // DDLL with optimization
                join(msg.pred, msg.succ, msg.req.successCallback, msg.req.failureCallback);
            } else {
                // DDLL without optimization
                int delay = 0;
                switch (Sim.retryMode.value()) {
                case IMMED:
                    delay = 0;
                    break;
                case RANDOM:
                    delay = Sim.rand.nextInt(JOIN_RETRY_DELAY)
                            * NetworkParams.HALFWAY_DELAY;
                    break;
                case CONST:
                    delay = JOIN_RETRY_DELAY * NetworkParams.HALFWAY_DELAY;
                    break;
                }
                if (delay == 0) {
                    n.joinUsingIntroducer(n.pred, msg.req.successCallback, msg.req.failureCallback);
                } else {
                    //n.post(new DdllJoinLater(n, delay, n.pred));
                    EventDispatcher.sched(delay, () -> {
                        if (status == DdllStatus.OUT) {
                            n.joinUsingIntroducer(msg.pred, msg.req.successCallback,
                                    msg.req.failureCallback);
                        }
                    });
                }
            }
        } else if (status == DdllStatus.DEL) {
            status = DdllStatus.IN;
            System.out.println(n + ": retry deletion:" + this.toStringDetail());
            System.out.println("pred: " + getPredecessor().toStringDetail());
            long delay = (long)(NetworkParams.ONEWAY_DELAY * Sim.rand.nextDouble());
            EventDispatcher.sched(delay, () -> {
                leave(leaveCallback, msg.req.successCallback);
            });
        }
    }

    //    public void ddlljoinlater(DdllJoinLater msg) {
    //        if (status == DdllStatus.OUT) {
    //            n.joinUsingIntroducer(msg.pred);
    //        }
    //    }

    public void setl(SetL msg) {
        if (lseq < msg.seq) {
            Node prevL = getPredecessor();
            n.setPred(msg.lNew);
            lseq = msg.seq;
            msg.nbrs.add(msg.lNew);
            leftNbrs.set(msg.nbrs);
            // 1 2 [3] 4
            // 自ノードが4とする．
            // [3]を挿入する場合，4がSetL受信．この場合のlimitは2 (lPrev=2, lNew=3)
            // [3]を削除する場合，4がSetL受信．この場合のlimitも2 (lPrev=3, lNew=2)
            if (Node.isOrdered(prevL.key, msg.lNew.key, n.key)) {
                // this SetL is sent for inserting a node (lNew)
                leftNbrs.sendRight(n.key, getSuccessor(), prevL.key);
            } else {
                // this SetL is sent for deleting a node (prevL)
                leftNbrs.sendRight(n.key, getSuccessor(), getPredecessor().key);
            }
        }
    }

    public void propagateNeighbors(PropagateNeighbors msg) {
        leftNbrs.receiveNeighbors(msg.src, msg.propset, getSuccessor(),
                msg.limit);
    }

    public void schedulePing() {
        if (pingPeriod.value() == 0
                || (status != DdllStatus.IN && status != DdllStatus.DEL)) {
            return;
        }
        EventDispatcher.sched(pingPeriod.value(), () -> {
            Node pred = getPredecessor();
            n.post(new Ping(pred, (Pong pong) -> {
                schedulePing();
            }), (exc) -> {
                fix(pred);
                schedulePing();
            });
        });
    }

    private Node getLiveNeighbor(boolean isLeftward) {
        LocalNode[] nodes = EventDispatcher.getNodes();
        int i = Arrays.binarySearch(nodes, n);
        if (i < 0) {
            throw new Error("should not happen");
        }
        int delta = isLeftward ? nodes.length - 1 : 1;
        i = (i + delta) % nodes.length;
        for (; nodes[i] != n; i = (i + delta) % nodes.length) {
            LocalNode x = nodes[i];
            DdllStrategy ds = (DdllStrategy) x.baseStrategy;
            if (x.mode != NodeMode.FAILED && (ds.status == DdllStatus.IN
                    || ds.status == DdllStatus.DEL)) {
                return x;
            }
        }
        return null;
    }

    /**
     * ノード故障からの修復をおこなう．
     * 
     * @param failed 故障ノード
     */
    public static void fix(Node failed) {
        throw new UnsupportedOperationException("fix() is not implemented");
        /*
        System.out.println("DDLL repair (failed=" + failed + ")");
        DdllStrategy fstr = ((DdllStrategy)failed.baseStrategy);
        Node newLeft = fstr.getLiveNeighbor(true);
        Node newRight = fstr.getLiveNeighbor(false);
        if (newLeft == null || newRight == null) {
            System.out.println(failed + ": fix() fails: newLeft="
                    + newLeft + ", newRight=" + newRight);
            EventDispatcher.dump();
            throw new Error(failed + ": fix() fails");
        }
        System.out.println(failed + ": fix() between " + newLeft
                + " and " + newRight);
        // XXX: なんちゃって修復
        newLeft.setSucc(newRight);
        newRight.setPred(newLeft);
        DdllStrategy leftstr = (DdllStrategy) newLeft.baseStrategy;
        DdllStrategy rightstr = (DdllStrategy) newRight.baseStrategy;
        rightstr.lseq++; // XXX: should be (g, s) form
        leftstr.rseq = rightstr.lseq;
        */
        //System.out.println(newLeft.toStringDetail() + "\n"
        // + newRight.toStringDetail());
    }

    @Override
    public int getMessages4Join() {
        return joinMsgs;
    }
}
