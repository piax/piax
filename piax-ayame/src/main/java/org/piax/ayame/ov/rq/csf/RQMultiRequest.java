package org.piax.ayame.ov.rq.csf;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.piax.ayame.EventExecutor;
import org.piax.ayame.LocalNode;
import org.piax.ayame.ov.rq.RQAdapter;
import org.piax.ayame.ov.rq.RQRange;
import org.piax.ayame.ov.rq.RQRequest;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.RetransMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RQMultiRequest<T> extends RQRequest<T> {
    /*--- logger ---*/
    private static final Logger logger =
            LoggerFactory.getLogger(RQMultiRequest.class);
	private static final long serialVersionUID = 1L;
	Set<RQRequest<T>> set = new HashSet<RQRequest<T>>();

	/*
	 * Create root RQMultirequest
	 */
	RQMultiRequest(RQRequest<T> req, Collection<RQRange> dest, RQAdapter<T> adapter) {
		super(req, dest, adapter);
	}

	/*
	 * Create sender-half of the RQMultiRequest argument
	 */
	RQMultiRequest(RQMultiRequest<T> req, Consumer<Throwable> errorHandler) {
		super(req, req.receiver, req.targetRanges, errorHandler);
		this.set = req.set;
	}
	
	RQMultiRequest<T> spawnSenderHalf() {
		return new RQMultiRequest<>(this,
                (Throwable th) -> {
                    logger.debug("{} for {}", th, this);
                    getLocalNode().addPossiblyFailedNode(receiver);
                    RetransMode mode = opts.getRetransMode();
                    if (mode == RetransMode.FAST || mode == RetransMode.RELIABLE) {
                        if (receiver == getLocalNode().succ) {
                            logger.debug("start fast retransmission! (delayed) {}", getTargetRanges());
                            EventExecutor.sched(
                                    "rq-retry-successor-failure",
                                    RQ_RETRY_SUCCESSOR_FAILURE_DELAY,
                                    () -> catcher.rqDisseminate((List<RQRange>)getTargetRanges()));
                        } else {
                            logger.debug("start fast retransmission! {}", getTargetRanges());
                            catcher.rqDisseminate((List<RQRange>)getTargetRanges());
                        }
                    }
                });
	}
	
	public void addRQRequest(RQRequest<T> storedreq) {
		set.add(storedreq);
	}

    public void postRQMultiRequest(LocalNode node) {
		beforeRunHook(node);
		catcher = new RQCatcher(targetRanges);
		RQMultiRequest<T> send = this.spawnSenderHalf();
		this.catcher.childMsgs.add(send);
		send.cleanup.add(() -> {
			boolean rc = this.catcher.childMsgs.remove(send);
			assert rc;
		});
		node.post(send);
		this.cleanup.add(() -> send.cleanup());
    }

	@Override
    public void run() {
		if (beforeRunHook(getLocalNode()))
				super.run();
		for (RQRequest<T> req: set) {
			// reset catcher
			RQRequest<T> receiver = (RQRequest<T>)req.clone();
			if (receiver.beforeRunHook(getLocalNode()))
				receiver.run();
		}
    }
}
