package org.piax.gtrans;

public class ConfigurationError extends Error {
    public ConfigurationError() {
    }

    /**
     * @param message the message string.
     */
    public ConfigurationError(String message) {
        super(message);
    }

    public ConfigurationError(Throwable cause) {
        super(cause);
    }
}
