package io.kestra.plugin.git;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * Shared helpers for the {@code namespaceDirectory} prefix used by {@link SyncNamespaceFiles} and
 * {@link PushNamespaceFiles} to scope Namespace Files to a subfolder of the namespace.
 *
 * <p>
 * Kept local to this repository (not in plugin-git-lib): it only encodes a convention specific to these two
 * concrete tasks, not shared plumbing.
 */
final class NamespaceDirectories {

    private NamespaceDirectories() {
    }

    /**
     * Normalizes a raw {@code namespaceDirectory} value into a canonical prefix.
     *
     * <p>
     * {@code "/"}, {@code ""}, {@code "."} and {@code null} all normalize to {@code ""} (no prefix); a leading
     * slash is optional on input and forced on output; {@code "."} segments (e.g. {@code "./shared-scripts"}) are
     * dropped like empty ones so they don't silently produce a prefix that matches nothing.
     *
     * <p>
     * {@code ".."} segments are rejected — both literal and percent-encoded (e.g. {@code "%2e%2e"}). The prefix is
     * later concatenated into a URI whose {@code getPath()} decodes percent-encoding
     * ({@link SyncNamespaceFiles#resolveTarget}), so an encoded {@code ".."} would otherwise decode back into a
     * parent-directory segment and let the destination path escape the intended namespace subtree.
     */
    static String normalize(String rendered) {
        if (rendered == null) {
            return "";
        }
        List<String> segments = Arrays.stream(rendered.trim().split("/"))
            .filter(segment -> !segment.isEmpty() && !".".equals(segment))
            .toList();
        String prefix = segments.isEmpty() ? "" : "/" + String.join("/", segments);
        if (containsParentSegment(prefix)) {
            throw new IllegalArgumentException(
                "Invalid 'namespaceDirectory' value '" + rendered + "': '..' path segments are not allowed."
            );
        }
        return prefix;
    }

    // Checks the prefix for a `..` segment after percent-decoding, matching how the prefix is later decoded when
    // concatenated into a URI in SyncNamespaceFiles.resolveTarget. Decoding here (a single pass, like getPath())
    // catches encoded traversal such as `%2e%2e` that a raw string check would miss.
    private static boolean containsParentSegment(String prefix) {
        if (prefix.isEmpty()) {
            return false;
        }
        String decodedPath;
        try {
            decodedPath = URI.create(prefix).getPath();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Invalid 'namespaceDirectory' value '" + prefix + "': not a valid path (" + e.getMessage() + ").", e
            );
        }
        return Arrays.asList(decodedPath.split("/")).contains("..");
    }

    /**
     * Whether a namespace-relative path (leading slash, e.g. {@code "/shared-scripts/foo.py"}) falls under the
     * given normalized {@code prefix}. An empty prefix (no scoping) matches everything.
     */
    static boolean isUnderPrefix(String path, String prefix) {
        if (prefix.isEmpty()) {
            return true;
        }
        return path.equals(prefix) || path.startsWith(prefix + "/");
    }

    /**
     * Relativizes a namespace-relative path (no leading slash, e.g. {@code "shared-scripts/foo.py"}) against
     * {@code prefix}, or returns {@code null} when the path falls outside it.
     */
    static String stripPrefix(String path, String prefix) {
        if (prefix.isEmpty()) {
            return path;
        }
        String normalizedPath = "/" + path;
        if (normalizedPath.equals(prefix)) {
            return "";
        }
        if (!normalizedPath.startsWith(prefix + "/")) {
            return null;
        }
        return normalizedPath.substring(prefix.length() + 1);
    }
}
