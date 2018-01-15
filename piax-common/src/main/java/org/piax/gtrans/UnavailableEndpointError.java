package org.piax.gtrans;

public class UnavailableEndpointError extends Error {
    public UnavailableEndpointError() {
    }

    /**
     * @param message the message string.
     */
    public UnavailableEndpointError(String message) {
        super(message);
    }

    /**
     * @param cause the cause of the error.
     */
    public UnavailableEndpointError(Throwable cause) {
        super(cause);
    }
}
