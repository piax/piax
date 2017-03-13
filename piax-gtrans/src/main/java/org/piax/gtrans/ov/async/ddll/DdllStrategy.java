package org.piax.gtrans.ov.async.ddll;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.async.Event.TimerEvent;
import org.piax.gtrans.async.EventException;
import org.piax.gtrans.async.EventException.RetriableException;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Log;
import org.piax.gtrans.async.NetworkParams;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.async.NodeStrategy;
import org.piax.gtrans.async.Option.EnumOption;
import org.piax.gtrans.async.Option.IntegerOption;
import org.piax.gtrans.ov.async.ddll.DdllEvent.GetCandidates;
import org.piax.gtrans.ov.async.ddll.DdllEvent.PropagateNeighbors;
import org.piax.gtrans.ov.async.ddll.DdllEvent.SetL;
import org.piax.gtrans.ov.async.ddll.DdllEvent.SetR;
import org.piax.gtrans.ov.async.ddll.DdllEvent.SetRAck;
import org.piax.gtrans.ov.async.ddll.DdllEvent.SetRAckNak;
import org.piax.gtrans.ov.async.ddll.DdllEvent.SetRJob;
import org.piax.gtrans.ov.async.ddll.DdllEvent.SetRNak;
import org.piax.gtrans.ov.ddll.LinkNum;

public class DdllStrategy extends NodeStrategy {
    public static class DdllNodeFactory extends NodeFactory {
        @Override
        public void setupNode(LocalNode node) {
            node.pushStrategy(new DdllStrategy());
        }
        @Override
        public String toString() {
            return "DDLL";
        }
    }

    public static enum DdllStatus {
        OUT, INS, DEL, IN
    };

    public static int JOIN_RETRY_DELAY = 2;

    public enum SetRNakMode {
        SETRNAK_NONE,
        /** SetRNakメッセージにsuccessor, predecessor情報を入れる．
            joinリトライ時にそれをそのまま使ってSetRを送信．*/
        SETRNAK_OPT1, 
        /** SetRNakメッセージにsuccessor, predecessor情報を入れる．
            joinリトライ時，predecessorに変化がなければそのまま使ってSetR送信．
            変わっていればSetRを送っても無駄なので再検索する．*/
        SETRNAK_OPT2
    };

    public enum SetRType {
        NORMAL, LEFTONLY, BOTH
    }

    public static EnumOption<SetRNakMode> setrnakmode = new EnumOption<>(
            SetRNakMode.class, SetRNakMode.SETRNAK_NONE, "-setrnak");

    public enum RetryMode {
        IMMED, CONST, RANDOM
    }
    public static EnumOption<RetryMode> retryMode
        = new EnumOption<>(RetryMode.class, RetryMode.IMMED, "-retrymode");

    // pinging is off by default
    public static IntegerOption pingPeriod =
            new IntegerOption(0, "-pingperiod");

    LinkNum lseq = new LinkNum(0, 0), rseq = new LinkNum(0, 0);
    DdllStatus status = DdllStatus.OUT;

    /** neighbor node set */
    public NeighborSet leftNbrs;

    private int joinMsgs = 0;

    public static void load() {
    }

    @Override
    public void activate(LocalNode node) {
        super.activate(node);
        this.leftNbrs = new NeighborSet(node);
    }

    public static DdllStrategy getDdllStrategy(LocalNode node) {
        return (DdllStrategy)node.getStrategy(DdllStrategy.class);
    }

    @Override
    public String toStringDetail() {
        return "N" + n.key + "(succ=" + (n.succ != null ? n.succ.key : "null")
                + ", pred=" + (n.pred != null ? n.pred.key : "null")
                + ", status=" + status + ", lseq=" + lseq + ", rseq=" + rseq
                + ", nbrs=" + leftNbrs + ")";
    }

    @Override
    public List<List<Node>> getRoutingEntries() {
        return Arrays.asList(n.pred, n, n.succ).stream()
                .distinct()
                .map(Collections::singletonList)
                .collect(Collectors.toList());
    }

    @Override
    public void initInitialNode() {
        n.succ = n;
        n.pred = n;
        setStatus(DdllStatus.IN);
        schedNextPing();
    }

    @Override
    public void join(LookupDone l, 
            CompletableFuture<Boolean> joinFuture) {
        join(l.pred, l.succ, joinFuture, null);
    }

    public void join(Node pred, Node succ,
            CompletableFuture<Boolean> joinComplete, SetRJob setRjob) {
        n.pred = pred;
        n.succ = succ;
        setStatus(DdllStatus.INS);
        SetR ev = new SetR(n.pred, SetRType.NORMAL, n, n.succ,
                new LinkNum(0, 0), setRjob);
        Consumer<EventException> joinfail = (exc) -> {
            System.out.println(n + ": join failed: " + exc);
            setStatus(DdllStatus.OUT);
            joinComplete.completeExceptionally(exc);
        };
        ev.onReply((SetRAckNak msg0, Throwable exc) -> {
            if (exc != null) {
                joinfail.accept((EventException)exc);
            } else if (msg0 instanceof SetRAck) {
                SetRAck msg = (SetRAck)msg0;
                joinMsgs += 3; // SetR, SetRAck and SetL
                setStatus(DdllStatus.IN);
                schedNextPing();
                rseq = msg.rnewnum;
                leftNbrs.set(msg.nbrs);
                // nbrs does not contain the immediate left node
                leftNbrs.add(getPredecessor());
                Log.verbose(() -> n + ": INSERTED, vtime = " + msg.vtime);
                joinComplete.complete(true);
            } else if (msg0 instanceof SetRNak){
                SetRNak msg = (SetRNak)msg0;
                joinMsgs += 2; // SetR and SetRNak
                setStatus(DdllStatus.OUT);
                // retry!
                Log.verbose(() -> "receive SetRNak: join retry, pred=" + msg.pred
                        + ", succ=" + msg.succ);
                if (setrnakmode.value() == SetRNakMode.SETRNAK_OPT2) {
                    // DDLLopt2
                    if (msg.pred == n.pred) {
                        join(msg.pred, msg.succ, joinComplete, setRjob);
                    } else {
                        joinfail.accept(new RetriableException("SetRNak1"));
                    }
                } else if (setrnakmode.value() == SetRNakMode.SETRNAK_OPT1) {
                    // DDLLopt1
                    join(msg.pred, msg.succ, joinComplete, setRjob);
                } else {
                    // DDLL without optimization
                    long delay = 0;
                    switch (retryMode.value()) {
                    case IMMED:
                        delay = 0;
                        break;
                    case RANDOM:
                        delay = EventExecutor.random().nextInt(JOIN_RETRY_DELAY)
                                * NetworkParams.HALFWAY_DELAY;
                        break;
                    case CONST:
                        delay = JOIN_RETRY_DELAY * NetworkParams.HALFWAY_DELAY;
                        break;
                    }
                    if (delay == 0) {
                        joinfail.accept(new RetriableException("SetRNak2"));
                    } else {
                        EventExecutor.sched(delay, () -> {
                            assert status == DdllStatus.OUT;
                            joinfail.accept(new RetriableException("SetRNak3"));
                        });
                    }
                }
            } else {
                throw new Error("shouldn't happen");
            }
         });
        n.post(ev);
    }

    @Override
    public void leave(CompletableFuture<Boolean> leaveComplete) {
        leave(leaveComplete, null);
    }

    public void leave(CompletableFuture<Boolean> leaveComplete,
            SetRJob setRjob) {
        System.out.println(n + ": leave start");
        stopPeriodicalPing();
        if (!fixComplete.isDone()) {
            System.out.println("but fix() is running");
            fixComplete.thenAccept(rc -> {
                if (rc) {
                    leave(leaveComplete, setRjob);
                } else {
                    // leave ungracefully
                    System.out.println("leave ungracefully");
                    leaveComplete.complete(false);
                }
            });
            return;
        }
        if (n.succ == n) {
            // last node case
            setStatus(DdllStatus.OUT);
            leaveComplete.complete(true);
            return;
        }
        setStatus(DdllStatus.DEL);
        SetR ev = new SetR(n.pred, SetRType.NORMAL, n.succ, n,
                rseq.next(), setRjob);
        ev.onReply((msg0, exc) -> {
            if (exc != null) {
                System.out.println(n + ": leave failed: " + exc);
                if (!(exc instanceof RetriableException)) {
                    // fix and retry
                    setStatus(DdllStatus.IN);
                    CompletableFuture<Boolean> fix = checkAndFix();
                    fix.thenRun(() -> leave(leaveComplete, setRjob));
                } else {
                    // just retry
                    leave(leaveComplete, setRjob);
                }
            } else if (msg0 instanceof SetRAck) {
                setStatus(DdllStatus.OUT);
                System.out.println(n + ": DELETED, vtime = " + msg0.vtime);
                leaveComplete.complete(true);
            } else if (msg0 instanceof SetRNak) {
                setStatus(DdllStatus.IN);
                System.out.println(n + ": retry deletion:" + this.toStringDetail());
                System.out.println("pred: " + getPredecessor().toStringDetail());
                long delay =
                        (long) (NetworkParams.ONEWAY_DELAY * EventExecutor.random().nextDouble());
                EventExecutor.sched(delay, () -> {
                    leave(leaveComplete, setRjob);
                });
            } else {
                throw new Error("shouldn't happen");
            }
        });
        n.post(ev);
    }

    private void setStatus(DdllStatus newStatus) {
        if (this.status != newStatus) {
            this.status = newStatus;
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
                EventExecutor.addCounter("SetR:!IN");
                n.post(new SetRNak(msg, null, null)); // XXX:
            } else {
                EventExecutor.addCounter("SetR:mismatch");
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
            if (msg.type != SetRType.NORMAL) {
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
                if (msg.type != SetRType.LEFTONLY) {
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

    @Override
    public void foundMaybeFailedNode(Node node) {
        System.out.println(n + ": foundFailedNode: " + node);
    }

    @Override
    public int getMessages4Join() {
        return joinMsgs;
    }

    /*
     * Link Recovery Part
     */
    private TimerEvent pingTimerEvent = null;
    enum FIXSTATE {IDLE, CHECKING, FIXING};
    private FIXSTATE fixState = FIXSTATE.IDLE;
    private CompletableFuture<Boolean> fixComplete = CompletableFuture.completedFuture(true);
    private void schedNextPing() {
        assert pingTimerEvent == null;
        if (pingPeriod.value() == 0 || status != DdllStatus.IN) {
            return;
        }
        pingTimerEvent = EventExecutor.sched(pingPeriod.value(), () -> {
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

    /*
     *   Idle <---------,
     *     ↓            | 
     *   Check <--,     |
     *     ↓      |Fail |Success
     *    Fix  ---'     |
     *     |            |
     *     `------------'
     * 
     */
    public CompletableFuture<Boolean> checkAndFix() {
        if (!fixComplete.isDone()) {
            // we have already doing the job
            return fixComplete;
        }
        fixComplete = checkAndFix0();
        return fixComplete;
    }
    private CompletableFuture<Boolean> checkAndFix0() {
        fixState = FIXSTATE.CHECKING;
        Log.verbose(() -> "FIXSTATE=" + fixState);
        CompletableFuture<Boolean> future = getLiveLeft()
                .thenCompose(nodes -> {
                    fixState = FIXSTATE.FIXING;
                    Log.verbose(() -> "FIXSTATE=" + fixState);
                    return fix(nodes[0], nodes[1]);
                }).thenCompose(rc -> {
                    if (!rc) {
                        return checkAndFix0();
                    } else {
                        fixState = FIXSTATE.IDLE;
                        Log.verbose(() -> "FIXSTATE=" + fixState);
                        return CompletableFuture.completedFuture(true);
                    }
                });
        return future;
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
                .filter(q -> !n.maybeFailedNodes.contains(q))
                .reduce((a, b) -> b).orElse(null);
        Node last0 = last;
        Log.verbose(() -> "left=" + left +", last=" + last0
                + ", suspect=" + n.maybeFailedNodes);
        if (last == left) {
            future.complete(new Node[]{left, leftSucc});
            return;
        }
        if (last == null) {
            last = n;
        }
        GetCandidates ev = new GetCandidates(last, n);
        ev.onReply((resp, exc) -> {
            if (exc != null) {
                // redo.  because the failed node should have been added to
                // suspectedNode, it is safe to redo.
                System.out.println(n + ": getLiveLeft: got " + exc);
                getLiveLeft(left, leftSucc, candidates, future);
            } else {
                getLiveLeft(resp.origin, resp.succ, resp.candidates, future);
            }
        });
        n.post(ev);
    }

    /**
     * @param left the closest live predecessor
     * @param leftSucc its successor
     * @return CompletableFuture
     */
    private CompletableFuture<Boolean> fix(Node left, Node leftSucc) {
        Log.verbose(() -> n + ": fix(" + left
                + ", " + leftSucc + "): status=" + status);
        if (status != DdllStatus.IN) {
            System.out.println("not IN");
            return CompletableFuture.completedFuture(true);
        }
        if (leftSucc == n) {
            Log.verbose(() -> "no problem");
            return CompletableFuture.completedFuture(true);
        }
        n.setPred(left);
        lseq = lseq.gnext();
        SetRType type;
        if (left != leftSucc && Node.isOrdered(left.key, true, leftSucc.key, n.key, false)) {
            System.out.println("seems " + left + " is dead");
            type = SetRType.LEFTONLY;
        } else {
            System.out.println("must join between " + left + " and " + leftSucc);
            type = SetRType.BOTH;
            n.setSucc(leftSucc);
        }
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        SetR ev = new SetR(left, type, n, leftSucc, lseq, null);
        ev.onReply((msg0, exc) -> {
            if (exc != null || msg0 instanceof SetRNak) {
                // while fixing fails, retry fixing 
                System.out.println(n + ": fix failed: "
                        + (exc == null ? "SetRNak" : exc));
                future.complete(false);
            } else if (msg0 instanceof SetRAck) {
                SetRAck msg = (SetRAck)msg0;
                leftNbrs.set(msg.nbrs);
                leftNbrs.add(getPredecessor());
                if (type != SetRType.LEFTONLY) {
                    rseq = msg.rnewnum;
                }
                System.out.println(n + ": fix completed");
                future.complete(true);
            } else {
                throw new Error("shouldn't happen");
            }
        });
        n.post(ev);
        return future;
    }
}
