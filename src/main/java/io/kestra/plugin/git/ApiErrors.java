package io.kestra.plugin.git;

/**
 * Small repo-local helper for turning {@code ApiException} details into user-facing messages.
 *
 * <p>
 * Not to be confused with the shared connection/diagnostics plumbing in {@code plugin-git-lib}: this only bounds
 * the raw HTTP response body that the SDK puts in {@code ApiException#getMessage()}, which is otherwise uncapped and
 * would embed an entire error page (or worse) into a thrown exception or a log line.
 */
final class ApiErrors {

    /** Maximum number of characters of an API response message to surface in an error or log. */
    static final int MAX_MESSAGE_LENGTH = 500;

    private ApiErrors() {
    }

    /**
     * Returns {@code message} bounded to {@link #MAX_MESSAGE_LENGTH} characters, appending a truncation marker when
     * it was longer. {@code null} becomes an empty string so callers can concatenate it safely.
     */
    static String truncate(String message) {
        if (message == null) {
            return "";
        }
        if (message.length() <= MAX_MESSAGE_LENGTH) {
            return message;
        }
        return message.substring(0, MAX_MESSAGE_LENGTH) + "… (truncated)";
    }
}
