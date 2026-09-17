package io.kestra.plugin.git;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Minimal in-process HTTP server simulating only the Kestra namespace lifecycle endpoints
 * ({@code GET}/{@code POST} {@code /api/v1/{tenant}/namespaces[/{id}]}).
 *
 * <p>
 * Unlike {@code io.kestra.plugin.git.shared.testkit.MockKestraApiServer} (from {@code plugin-git-lib}),
 * this fixture models real Kestra namespace lifecycle semantics, which {@link SyncNamespaceFiles} relies on:
 * {@code GET namespaces/{id}} returns {@code 200} with a synthetic body even for a namespace that was never
 * persisted (it never 404s), so existence cannot be checked with a GET; {@code POST namespaces} is the source of
 * truth. Kestra Enterprise Edition returns {@code 422} with a validation-error body (not {@code 409}) when the
 * namespace already exists, so that is what this mock reproduces for the already-exists case.
 */
public class NamespaceLifecycleMockServer implements AutoCloseable {
    private static final Pattern NAMESPACE_ID_PATTERN = Pattern.compile("\"id\"\\s*:\\s*\"([^\"]+)\"");

    private final HttpServer server;
    private final Set<String> existingNamespaces = ConcurrentHashMap.newKeySet();
    private final List<String> createAttempts = new CopyOnWriteArrayList<>();
    private final List<String> createdNamespaces = new CopyOnWriteArrayList<>();

    public NamespaceLifecycleMockServer(String... initiallyExistingNamespaces) throws IOException {
        this.existingNamespaces.addAll(List.of(initiallyExistingNamespaces));
        this.server = HttpServer.create(new InetSocketAddress(0), 0);
        this.server.createContext("/api/v1/", this::handle);
        this.server.start();
    }

    public String url() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    /** Namespace ids for which a {@code POST namespaces} call was received, in call order, whatever the outcome. */
    public List<String> createAttempts() {
        return List.copyOf(createAttempts);
    }

    /** Namespace ids that were actually newly created (a create attempt that did not 409), in call order. */
    public List<String> createdNamespaces() {
        return List.copyOf(createdNamespaces);
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        var path = exchange.getRequestURI().getPath();
        var method = exchange.getRequestMethod();
        // Path shape: /api/v1/{tenant}/namespaces[/{id}]
        var parts = path.split("/");

        if ("GET".equals(method) && parts.length >= 6 && "namespaces".equals(parts[4])) {
            // Real Kestra 2.0.0 behavior: a synthetic 200 body, never a 404, even for an unknown id.
            exchange.getRequestBody().readAllBytes();
            sendJson(exchange, 200, "{\"id\":\"" + decode(parts[5]) + "\"}");
            return;
        }

        if ("POST".equals(method) && parts.length >= 5 && "namespaces".equals(parts[4])) {
            var body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            var idMatcher = NAMESPACE_ID_PATTERN.matcher(body);
            var id = idMatcher.find() ? idMatcher.group(1) : "unknown";
            createAttempts.add(id);

            if (!existingNamespaces.add(id)) {
                // Real EE response shape for an already-existing namespace id.
                sendJson(exchange, 422, """
                    {"type":".../validation-failed","title":"Validation failed","status":422,\
                    "detail":"namespace.id: Namespace id already exists","errors":[\
                    {"detail":"Namespace id already exists","pointer":"/namespace/id","path":"namespace.id"}]}""");
                return;
            }
            createdNamespaces.add(id);
            sendJson(exchange, 200, "{\"id\":\"" + id + "\"}");
            return;
        }

        exchange.getRequestBody().readAllBytes();
        sendJson(exchange, 200, "{}");
    }

    private static String decode(String s) {
        try {
            return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }

    private static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        var bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
