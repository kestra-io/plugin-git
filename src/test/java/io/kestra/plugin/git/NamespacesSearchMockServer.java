package io.kestra.plugin.git;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.sun.net.httpserver.HttpServer;

/**
 * Minimal in-process stand-in for the Kestra {@code GET /api/v1/{tenant}/namespaces/search} endpoint used only to
 * exercise {@code includeChildNamespaces} in {@link SyncNamespaceFilesTest} / {@link PushNamespaceFilesTest}.
 *
 * <p>
 * {@code MockKestraApiServer} (plugin-git-lib test fixtures) backs the flow endpoints used by
 * SyncFlows/PushFlows, but Namespace Files tasks resolve descendant namespaces through a separate namespaces-search
 * endpoint that it does not implement (unmatched requests fall through to its generic `{}` response, so
 * `descendantNamespaces()` silently returns no children). This local, test-only server fills that gap without
 * touching the shared plugin-git-lib test fixtures.
 */
final class NamespacesSearchMockServer implements AutoCloseable {
    private final HttpServer server;

    private NamespacesSearchMockServer(List<String> namespaceIds) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/", exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            String results = namespaceIds.stream()
                .map(id -> "{\"id\":\"" + id + "\"}")
                .reduce((a, b) -> a + "," + b)
                .orElse("");
            String json = "{\"results\":[" + results + "],\"total\":" + namespaceIds.size() + "}";
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        this.server.start();
    }

    static NamespacesSearchMockServer start(List<String> namespaceIds) throws IOException {
        return new NamespacesSearchMockServer(namespaceIds);
    }

    String url() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
