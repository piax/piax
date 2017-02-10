package org.piax.gtrans.ov.ddllasync;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.async.Event.TimerEvent;
import org.piax.gtrans.async.EventDispatcher;
import org.piax.gtrans.async.EventException.RetriableException;
import org.piax.gtrans.async.FailureCallback;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.NetworkParams;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.async.NodeStrategy;
import org.piax.gtrans.async.Option.EnumOption;
import org.piax.gtrans.async.Option.IntegerOption;
import org.piax.gtrans.async.Sim;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.LinkNum;
import org.piax.gtrans.ov.ddllasync.DdllEvent.GetCandidates;
import org.piax.gtrans.ov.ddllasync.DdllEvent.PropagateNeighbors;
import org.piax.gtrans.ov.ddllasync.DdllEvent.SetL;
import org.piax.gtrans.ov.ddllasync.DdllEvent.SetR;
import org.piax.gtrans.ov.ddllasync.DdllEvent.SetRAck;
import org.piax.gtrans.ov.ddllasync.DdllEvent.SetRJob;
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

    public enum FixType {
        NORMAL, LEFTONLY, BOTH
    }

    public static EnumOption<SetRNakMode> setrnakmode = new EnumOption<>(
            SetRNakMode.class, SetRNakMode.SETRNAK_NONE, "-setrnak");
    // pinging is off by default
    public static IntegerOption pingPeriod =
            new IntegerOption(0, "-pingperiod");

    LinkNum lseq = new LinkNum(0, 0), rseq = new LinkNum(0, 0);
    DdllStatus status = DdllStatus.OUT;

    CompletableFuture<Boolean> fixCompleteFuture = null;

    /** neighbor node set */
    public NeighborSet leftNbrs;

    // TODO: purge entries by timer! 2017/02/09
    Set<Node> suspectedNodes = new HashSet<>();

    public int joinTime = -1;
    private int joinMsgs = 0;

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

    @Override
    public void initInitialNode() {
        n.succ = n;
        n.pred = n;
        setStatus(DdllStatus.IN);
        schedNextPing();
    }

    @Override
    public void joinAfterLookup(LookupDone l, Runnable success,
            FailureCallback eh) {
        join(l.pred, l.succ, success, eh, null);
    }

    public void join(Node pred, Node succ, Runnable success, FailureCallback eh,
            SetRJob setRjob) {
        assert fixCompleteFuture == null;
        n.pred = pred;
        n.succ = succ;
        setStatus(DdllStatus.INS);
        SetR ev = new SetR(n.pred, DdllStrategy.FixType.NORMAL, n, n.succ,
                new LinkNum(0, 0), setRjob, success);
        n.post(ev, (exc) -> {
            System.out.println(n + ": join failed: " + exc);
            setStatus(DdllStatus.OUT);
            if (eh != null) {
                eh.run(exc);
            }
        });
    }

    @Override
    public void leave(Runnable callback) {
        leave(callback, null);
    }

    public void leave(Runnable success, SetRJob setRjob) {
        System.out.println(n + ": leave start");
        stopPeriodicalPing();
        if (fixCompleteFuture != null) {
            System.out.println("but fix() is running");
            fixCompleteFuture.thenAccept(rc -> {
                if (rc) {
                    leave(success, setRjob);
                } else {
                    checkAndFix().thenRun(() -> leave(success, setRjob));
                }
            });
            return;
        }
        setStatus(DdllStatus.DEL);
        SetR ev = new SetR(n.pred, FixType.NORMAL, n.succ, n,
                rseq.next(), setRjob, success);
        n.post(ev, (exc) -> {
            System.out.println(n + ": leave failed: " + exc);
            if (!(exc instanceof RetriableException)) {
                setStatus(DdllStatus.IN);
                CompletableFuture<Boolean> future = checkAndFix();
                future.thenRun(() -> leave(success, setRjob));
            } else {
                leave(success, setRjob); // and retry!
            }
        });
    }

    private void setStatus(DdllStatus newStatus) {
        if (this.status != newStatus) {
            this.status = newStatus;
            if (newStatus == DdllStatus.IN) {
                //schedNextPing();
            }
        }
    }

    public DdllStatus getStatus() {
        return status;
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
            if (msg.type != FixType.NORMAL) {
                leftNbrs.removeNode(msg.rCur);
                leftNbrs.add(msg.rNew);
            } else {
                if (forInsertion) {
                    leftNbrs.add(msg.rNew);
                } else {
                    leftNbrs.removeNode(msg.rCur);
                }
            }
            // compute a neighbor node set to send to the new right node 
            Set<Node> nset = leftNbrs.computeNSForRight(msg.rNew);
            if (forInsertion) {
                if (msg.type != FixType.LEFTONLY) {
                    Set<Node> nset2 =
                            leftNbrs.computeNSForRight(getSuccessor());
                    n.post(new SetL(n.succ, msg.rNew, rseq.next(), nset2));
                }
            } else {
                n.post(new SetL(msg.rNew, n, msg.rnewseq, nset));
            }
            leftNbrs.setPrevRightSet(msg.rNew, nset);
            n.post(new SetRAck(msg, rseq.next(), nset));
            n.setSucc(msg.rNew);
            rseq = msg.rnewseq;
            if (msg.setRJob != null) {
                msg.setRJob.run(n);
            }
        }
    }

    public void setrack(SetRAck msg) {
        if (fixCompleteFuture != null) {
            leftNbrs.set(msg.nbrs);
            leftNbrs.add(getPredecessor());
            if (msg.req.type != FixType.LEFTONLY) {
                rseq = msg.rnewnum;
            }
            System.out.println(n + ": fix completed");
            if (msg.req.success != null) {
                msg.req.success.run();
            }
            CompletableFuture<Boolean> f = fixCompleteFuture;
            fixCompleteFuture = null;
            f.complete(true);
            return;
        }
        switch (status) {
        case INS:
            joinMsgs += 3; // SetR, SetRAck and SetL
            setStatus(DdllStatus.IN);
            schedNextPing();
            rseq = msg.rnewnum;
            leftNbrs.set(msg.nbrs);
            // nbrs does not contain the immediate left node
            leftNbrs.add(getPredecessor());
            System.out.println(n + ": INSERTED, vtime = " + msg.vtime
                    + ", latency=" + n.latency);
            if (msg.req.success != null) {
                msg.req.success.run();
            }
            break;
        case DEL:
            setStatus(DdllStatus.OUT);
            System.out.println(n + ": DELETED, vtime = " + msg.vtime
                    + ", latency=" + n.latency);
            if (msg.req.success != null) {
                msg.req.success.run();
            }
            break;
        default:
            System.out.println("SetRAck received while status=" + status);
            break;
        }
    }

    public void setrnak(SetRNak msg) {
        if (fixCompleteFuture != null) {
            // fix attempt failed
            // XXX: not tested
            CompletableFuture<Boolean> f = fixCompleteFuture;
            fixCompleteFuture = null;
            f.complete(false);
            checkAndFix();
            return;
        }
        if (status == DdllStatus.INS) {
            joinMsgs += 2; // SetR and SetRNak
            setStatus(DdllStatus.OUT);
            // retry!
            LocalNode.verbose("receive SetRNak: join retry, pred=" + msg.pred
                    + ", succ=" + msg.succ);
            if (setrnakmode.value() == SetRNakMode.SETRNAK_OPT2) {
                // DDLL with optimization2
                if (msg.pred == n.pred) {
                    join(msg.pred, msg.succ, msg.req.success,
                            msg.req.failureCallback, msg.req.setRJob);
                } else {
                    msg.req.failureCallback
                            .run(new RetriableException("SetRNak1"));
                }
            } else if (setrnakmode.value() == SetRNakMode.SETRNAK_OPT1) {
                // DDLL with optimization
                join(msg.pred, msg.succ, msg.req.success,
                        msg.req.failureCallback, msg.req.setRJob);
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
                    msg.req.failureCallback
                            .run(new RetriableException("SetRNak2"));
                } else {
                    EventDispatcher.sched(delay, () -> {
                        if (status == DdllStatus.OUT) {
                            msg.req.failureCallback
                                    .run(new RetriableException("SetRNak3"));
                        }
                    });
                }
            }
        } else if (status == DdllStatus.DEL) {
            setStatus(DdllStatus.IN);
            System.out.println(n + ": retry deletion:" + this.toStringDetail());
            System.out.println("pred: " + getPredecessor().toStringDetail());
            long delay =
                    (long) (NetworkParams.ONEWAY_DELAY * Sim.rand.nextDouble());
            EventDispatcher.sched(delay, () -> {
                leave(msg.req.success, msg.req.setRJob);
            });
        }
    }

    //    public void ddlljoinlater(DdllJoinLater msg) {
    //        if (status == DdllStatus.OUT) {
    //            n.joinUsingIntroducer(msg.pred);
    //        }
    //    }

    public void setl(SetL msg) {
        if (lseq.compareTo(msg.seq) < 0) {
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
    
    /*
     * PINGに関するメモ:
     * 
     * SetRメッセージを送信中は，応答が返るまでcompletableFuture f を保持しておく．
     * SetRを送信する際，f が non null ならば，f が終わるまで待つ．
     * 
     * leave(): 
     * - PINGタイマを停止
     * - f が non null ならば f が終了するまで待つ．
     *   - 正常終了(SetRAck)ならば，leave再実行
     *   - 異常終了ならば，checkAndFix()を実行し，終了したらleave再実行
     * 
     * checkAndFix():
     * - sched(() -> fix())
     * - fixが終わったら再実行
     * 
     * fix():
     * - f が non null ならばreturn
     *   (このとき，leaveが実行されているはず)
     * 
     */
    private TimerEvent pingTimerEvent = null;
    private void schedNextPing() {
        assert pingTimerEvent == null;
        if (pingPeriod.value() == 0 || status != DdllStatus.IN) {
            return;
        }
        pingTimerEvent = EventDispatcher.sched(pingPeriod.value(), () -> {
            pingTimerEvent = null;
            checkAndFix()
                .thenRun(() -> schedNextPing());
        });
    }

    private void stopPeriodicalPing() {
        if (pingTimerEvent != null) {
            pingTimerEvent.cancel();
            pingTimerEvent = null;
        }
    }

    private CompletableFuture<Boolean> fixFuture = null;
    public CompletableFuture<Boolean> checkAndFix() {
        if (fixFuture != null) {
            // we have already doing the job
            return fixFuture;
        }
        fixFuture = getLiveLeft()
                .thenCompose(nodes -> fix(nodes[0], nodes[1]))
                .thenApply(rc -> {
                    fixFuture = null;
                    return rc;
                });
        return fixFuture;
    }

    private CompletableFuture<Node[]> getLiveLeft() {
        CompletableFuture<Node[]> future = new CompletableFuture<>();
        List<Node> cands = n.getNodesForFix(n.key);
        getLiveLeft(n, n.succ, cands, future);
        return future;
    }

    private void getLiveLeft(Node left, Node leftSucc, List<Node> candidates,
            CompletableFuture<Node[]> future) {
        Node last = candidates.stream()
                .filter(q -> !suspectedNodes.contains(q))
                .reduce((a, b) -> b).orElse(null);
        System.out.println("left=" + left +", last=" + last + ", susp" + suspectedNodes);
        if (last == left) {
            future.complete(new Node[]{left, leftSucc});
            return;
        }
        if (last == null) {
            last = n;
        }
        n.post(new GetCandidates(last, n, r -> {
            getLiveLeft(r.origin, r.succ, r.candidates, future);
        }), exc -> {
            // redo.  because the failed node should have been added to
            // suspectedNode, it is safe to redo.
            System.out.println(n + ": getLiveLeft: got " + exc);
            getLiveLeft(left, leftSucc, candidates, future);
        });
    }

    /**
     * @param left the closest live predecessor
     * @param leftSucc its successor
     * @return CompletableFuture
     */
    private CompletableFuture<Boolean> fix(Node left, Node leftSucc) {
        System.out.println(
                n + ": " + status + ": found " + left + ", " + leftSucc);
        if (status != DdllStatus.IN) {
            return CompletableFuture.completedFuture(true);
        }
        if (leftSucc == n) {
            // we have no problem
            System.out.println("no problem");
            return CompletableFuture.completedFuture(true);
        }
        assert fixCompleteFuture == null;
        fixCompleteFuture = new CompletableFuture<>();
        n.setPred(left);
        lseq = lseq.gnext();
        DdllStrategy.FixType type;
        if (left != leftSucc && Node.isOrdered(left.key, true, leftSucc.key, n.key, false)) {
            System.out.println("seems " + leftSucc + " is dead");
            type = DdllStrategy.FixType.LEFTONLY;
        } else {
            System.out.println("must join between " + left + " and " + leftSucc);
            type = DdllStrategy.FixType.BOTH;
            n.setSucc(leftSucc);
        }
        Event ev = new SetR(left, type, n, leftSucc, lseq, null, null);
        n.post(ev, (exc) -> {
            System.out.println(n + ": fix: SetRAck/Nak error: " + exc);
            CompletableFuture<Boolean> f = fixCompleteFuture;
            fixCompleteFuture = null;
            f.complete(true);
        });
        return fixCompleteFuture;
    }

    private Node getLiveNeighbor(boolean isLeftward) {
        LocalNode[] nodes = Sim.getNodes();
        int i = Arrays.binarySearch(nodes, n);
        if (i < 0) {
            throw new Error("should not happen");
        }
        int delta = isLeftward ? nodes.length - 1 : 1;
        i = (i + delta) % nodes.length;
        for (; nodes[i] != n; i = (i + delta) % nodes.length) {
            LocalNode x = nodes[i];
            DdllStrategy ds = (DdllStrategy) x.baseStrategy;
            if (!x.isFailed() && (ds.status == DdllStatus.IN
                    || ds.status == DdllStatus.DEL)) {
                return x;
            }
        }
        return null;
    }

    @Override
    public void foundFailedNode(Node node) {
        System.out.println(n + ": foundFailedNode: " + node);
        suspectedNodes.add(node);
    }

    @Override
    public int getMessages4Join() {
        return joinMsgs;
    }
}
