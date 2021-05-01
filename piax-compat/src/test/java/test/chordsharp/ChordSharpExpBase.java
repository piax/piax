/*
 * Revision History:
 * ---
 * 2009/02/13 designed and implemented by M. Yoshida.
 * 
 * $Id: TestAggSG.java 823 2013-08-25 00:41:53Z yos $
 */

package test.chordsharp;

import java.io.IOException;

import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.PeerLocator;
import org.piax.gtrans.ov.ddll.NeighborSet;
import org.piax.gtrans.ov.szk.ChordSharp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ChordSharpExpBase extends
        RingExpBase<ChordSharp<PeerLocator>> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(ChordSharpExpBase.class);
    
    static {
        NeighborSet.setDefaultNeighborSetSize(3);
    }

    public ChordSharpExpBase(int nodes) {
        super(nodes);
    }

    @Override
    protected ChordSharp<PeerLocator> createNode(ChannelTransport trans, int n)
            throws IOException, IdConflictException {
        System.out.println("Chord# node created");
        return new ChordSharp<PeerLocator>(new TransportId("chord#"), trans,
                null);
    }
}
