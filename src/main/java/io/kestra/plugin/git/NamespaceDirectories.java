package io.kestra.plugin.git;

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
     */
    static String normalize(String rendered) {
        if (rendered == null) {
            return "";
        }
        List<String> segments = Arrays.stream(rendered.trim().split("/"))
            .filter(segment -> !segment.isEmpty() && !".".equals(segment))
            .toList();
        if (segments.contains("..")) {
            throw new IllegalArgumentException(
                "Invalid 'namespaceDirectory' value '" + rendered + "': '..' path segments are not allowed."
            );
        }
        return segments.isEmpty() ? "" : "/" + String.join("/", segments);
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
