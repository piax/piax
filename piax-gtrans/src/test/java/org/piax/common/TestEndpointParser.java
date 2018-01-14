package org.piax.common;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.piax.gtrans.UnavailableEndpointError;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.idtrans.PrimaryKey;

class TestEndpointParser {

    @Test
    void testPrimaryKey() {
        Endpoint ep = Endpoint.newEndpoint("id:10.0:tcp:localhost:12367");
        assertTrue(ep instanceof PrimaryKey);
    }
    
    @Test
    void testLocator() {
        Endpoint ep = EndpointParser.parse("tcp:localhost:12367");
        assertTrue(ep instanceof NettyLocator);
        ep = EndpointParser.parse("udt:localhost:12367");
        assertTrue(ep instanceof NettyLocator);
        ep = EndpointParser.parse("ssl:localhost:12367");
        assertTrue(ep instanceof NettyLocator);
    }
    
    @Test
    void testUnloadedLocators() {
        // Not loaded in the piax-gtrans artifact.
        assertThrows(UnavailableEndpointError.class, ()->{
            EndpointParser.parse("-tcp:localhost:12367");
        }
        , "a message");
    }
    
}
