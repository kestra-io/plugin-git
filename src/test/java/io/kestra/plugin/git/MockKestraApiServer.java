package io.kestra.plugin.git;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.models.dashboards.Dashboard;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.repositories.DashboardRepositoryInterface;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.tenant.TenantService;

/**
 * In-process HTTP server that bridges Kestra SDK calls to in-memory repository beans.
 * Used in integration tests to avoid requiring a real Kestra server.
 */
public class MockKestraApiServer implements AutoCloseable {

    // SDK serializes QueryFilter as filters[namespace][EQUALS]=... or filters[namespace][STARTS_WITH]=...
    private static final Pattern NS_EQUALS_PATTERN = Pattern.compile("[&?]?filters%5Bnamespace%5D%5BEQUALS%5D=([^&]+)");
    private static final Pattern NS_STARTS_WITH_PATTERN = Pattern.compile("[&?]?filters%5Bnamespace%5D%5BSTARTS_WITH%5D=([^&]+)");
    private static final Pattern ID_EQUALS_PATTERN = Pattern.compile("[&?]?filters%5Bid%5D%5BEQUALS%5D=([^&]+)");
    private static final Pattern FLOW_ID_PATTERN = Pattern.compile("(?m)^id:\\s*(.+)$");
    private static final Pattern FLOW_NS_PATTERN = Pattern.compile("(?m)^namespace:\\s*(.+)$");
    private static final Pattern FLOW_REVISION_PATTERN = Pattern.compile("(?m)^revision:\\s*(.+)$");
    // Matches multipart boundary lines to extract file content
    private static final Pattern MULTIPART_BOUNDARY_PATTERN = Pattern.compile("boundary=([^\\r\\n;]+)");

    private final HttpServer server;
    private final FlowRepositoryInterface flowRepo;
    private final DashboardRepositoryInterface dashboardRepo;

    private MockKestraApiServer(FlowRepositoryInterface flowRepo, DashboardRepositoryInterface dashboardRepo) throws IOException {
        this.flowRepo = flowRepo;
        this.dashboardRepo = dashboardRepo;
        this.server = HttpServer.create(new InetSocketAddress(0), 0);
        registerHandlers();
        this.server.start();
    }

    /**
     * Creates and starts a mock server backed by the given repositories.
     * Call {@link #close()} when the test finishes.
     */
    public static MockKestraApiServer start(FlowRepositoryInterface flowRepo, DashboardRepositoryInterface dashboardRepo) throws IOException {
        return new MockKestraApiServer(flowRepo, dashboardRepo);
    }

    /**
     * Creates and starts a mock server backed only by the flow repository (dashboards not needed).
     */
    public static MockKestraApiServer start(FlowRepositoryInterface flowRepo) throws IOException {
        return new MockKestraApiServer(flowRepo, null);
    }

    /**
     * Creates and starts a mock server backed only by the dashboard repository (flows not needed).
     */
    public static MockKestraApiServer start(DashboardRepositoryInterface dashboardRepo) throws IOException {
        return new MockKestraApiServer(null, dashboardRepo);
    }

    /** Returns the base URL of this mock server, e.g. {@code http://localhost:54321}. */
    public String url() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    // ---- handler registration -----------------------------------------------

    private void registerHandlers() {
        // flows: export, import, validate, delete
        server.createContext("/api/v1/", exchange -> {
            var path = exchange.getRequestURI().getPath();
            var method = exchange.getRequestMethod();

            try {
                if (path.contains("/flows/export/by-query") && "GET".equals(method)) {
                    handleExportFlows(exchange);
                } else if (path.contains("/flows/import") && "POST".equals(method)) {
                    handleImportFlows(exchange);
                } else if (path.contains("/flows/validate") && "POST".equals(method)) {
                    handleValidateFlows(exchange);
                } else if (path.contains("/flows/") && "DELETE".equals(method) && !path.contains("/flows/delete")) {
                    handleDeleteFlow(exchange);
                } else if (path.contains("/namespaces/") && "GET".equals(method) && !path.contains("/secrets") && !path.contains("/plugindefaults") && !path.contains("/inherited")) {
                    handleGetNamespace(exchange);
                } else if (path.contains("/dashboards") && "GET".equals(method) && !path.contains("/charts") && !path.contains("/export")) {
                    handleSearchOrGetDashboard(exchange, path);
                } else if (path.contains("/dashboards") && "POST".equals(method)) {
                    handleCreateDashboard(exchange, path);
                } else if (path.contains("/dashboards/") && "DELETE".equals(method)) {
                    handleDeleteDashboard(exchange, path);
                } else {
                    // Unknown — return 200 with empty JSON to avoid connection-refused failures
                    sendJson(exchange, 200, "{}");
                }
            } catch (Exception e) {
                try {
                    sendJson(exchange, 500, "{\"error\": \"" + e.getMessage().replace("\"", "'") + "\"}");
                } catch (Exception ignored) {
                    // best effort
                }
            }
        });
    }

    // ---- flow handlers -------------------------------------------------------

    /**
     * GET /api/v1/{tenantId}/flows/export/by-query?filters%5Bnamespace%5D%5BEQUALS%5D=...
     * Returns a ZIP archive of flow YAML files from the in-memory repo.
     * The SDK serializes QueryFilter as filters[field][OP]=value (URL-encoded brackets).
     */
    private void handleExportFlows(HttpExchange exchange) throws IOException {
        var rawQuery = exchange.getRequestURI().getRawQuery();
        var tenantId = extractTenantId(exchange.getRequestURI().getPath(), "flows");

        // Parse namespace filter: SDK sends filters[namespace][EQUALS]=... or filters[namespace][STARTS_WITH]=...
        // The brackets are URL-encoded as %5B and %5D
        String namespace = null;
        boolean startsWithMode = false;
        if (rawQuery != null) {
            Matcher nsEqMatcher = NS_EQUALS_PATTERN.matcher(rawQuery);
            Matcher nsSwMatcher = NS_STARTS_WITH_PATTERN.matcher(rawQuery);
            if (nsSwMatcher.find()) {
                startsWithMode = true;
                namespace = decode(nsSwMatcher.group(1));
            } else if (nsEqMatcher.find()) {
                namespace = decode(nsEqMatcher.group(1));
            }
        }

        // Parse optional ID filter: filters[id][EQUALS]=...
        String flowIdFilter = null;
        if (rawQuery != null) {
            Matcher idMatcher = ID_EQUALS_PATTERN.matcher(rawQuery);
            if (idMatcher.find()) {
                flowIdFilter = decode(idMatcher.group(1));
            }
        }

        if (flowRepo == null) {
            sendJson(exchange, 200, "{}");
            return;
        }

        List<FlowWithSource> flows;
        if (namespace == null || namespace.isBlank()) {
            flows = flowRepo.findAllWithSourceForAllTenants();
        } else if (startsWithMode) {
            flows = flowRepo.findByNamespacePrefixWithSource(tenantId, namespace);
        } else {
            flows = flowRepo.findByNamespaceWithSource(tenantId, namespace);
        }

        // Apply optional ID filter
        if (flowIdFilter != null) {
            var fId = flowIdFilter;
            flows = flows.stream().filter(f -> fId.equals(f.getId())).toList();
        }

        // Deduplicate: keep only the latest revision per (namespace, id)
        Map<String, FlowWithSource> latest = new LinkedHashMap<>();
        for (var f : flows) {
            String key = f.getNamespace() + "." + f.getId();
            FlowWithSource existing = latest.get(key);
            if (existing == null || (f.getRevision() != null && (existing.getRevision() == null || f.getRevision() > existing.getRevision()))) {
                latest.put(key, f);
            }
        }
        flows = new ArrayList<>(latest.values());

        var baos = new ByteArrayOutputStream();
        try (var zos = new ZipOutputStream(baos)) {
            for (var f : flows) {
                var source = f.getSource();
                if (source == null || source.isBlank()) {
                    source = "id: " + f.getId() + "\nnamespace: " + f.getNamespace() + "\n";
                }
                var bytes = source.getBytes(StandardCharsets.UTF_8);
                zos.putNextEntry(new ZipEntry(f.getNamespace() + "." + f.getId() + ".yml"));
                zos.write(bytes);
                zos.closeEntry();
            }
        }

        var body = baos.toByteArray();
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    /**
     * POST /api/v1/{tenantId}/flows/import (multipart/form-data with fileUpload field)
     * Parses the YAML from the multipart body and saves it to the in-memory repo.
     */
    private void handleImportFlows(HttpExchange exchange) throws IOException {
        var tenantId = extractTenantId(exchange.getRequestURI().getPath(), "flows");
        var contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        var body = exchange.getRequestBody().readAllBytes();

        String yaml = extractYamlFromMultipart(contentType, body);
        if (flowRepo != null && yaml != null && !yaml.isBlank()) {
            importYaml(tenantId, yaml);
        }

        var responseBody = "[]".getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, responseBody.length);
        exchange.getResponseBody().write(responseBody);
        exchange.close();
    }

    /**
     * POST /api/v1/{tenantId}/flows/validate (text/yaml body)
     * Returns validation results. Detects invalid flows by checking for "unknown." type prefix.
     */
    private void handleValidateFlows(HttpExchange exchange) throws IOException {
        var body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        exchange.getRequestBody().close();

        // Detect invalid flow: any task type that looks like "unknown.*" is treated as invalid
        boolean isInvalid = body.contains("type: unknown.") || body.contains("type: io.kestra.plugin.core.log.Invalid");

        String jsonResponse;
        if (isInvalid) {
            // Return a violation with a non-null constraints field
            jsonResponse = """
                [{"index":0,"constraints":"Invalid task type: unknown or unregistered plugin","flow":null,"namespace":null}]
                """.strip();
        } else {
            // Return a single entry with null constraints (valid)
            jsonResponse = """
                [{"index":0,"constraints":null,"flow":null,"namespace":null}]
                """.strip();
        }

        var responseBody = jsonResponse.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, responseBody.length);
        exchange.getResponseBody().write(responseBody);
        exchange.close();
    }

    /**
     * DELETE /api/v1/{tenantId}/flows/{namespace}/{id}
     * Deletes the flow from the in-memory repo.
     */
    private void handleDeleteFlow(HttpExchange exchange) throws IOException {
        exchange.getRequestBody().readAllBytes();

        if (flowRepo != null) {
            var path = exchange.getRequestURI().getPath();

            // Path: /api/v1/{tenant}/flows/{namespace}/{id}
            var parts = path.split("/");
            // parts[0]="" [1]="api" [2]="v1" [3]=tenant [4]="flows" [5]=namespace [6]=id
            if (parts.length >= 7) {
                var tenantId = decode(parts[3]);
                var namespace = decode(parts[5]);
                var flowId = decode(parts[6]);
                var flows = flowRepo.findByNamespaceWithSource(tenantId, namespace).stream()
                    .filter(f -> flowId.equals(f.getId()))
                    .toList();
                for (var flow : flows) {
                    flowRepo.delete(flow);
                }
            }
        }

        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    /**
     * GET /api/v1/{tenantId}/namespaces/{id}
     * Returns a minimal namespace JSON to indicate it exists.
     */
    private void handleGetNamespace(HttpExchange exchange) throws IOException {
        exchange.getRequestBody().readAllBytes();
        var path = exchange.getRequestURI().getPath();
        var parts = path.split("/");
        // parts: ["", "api", "v1", tenant, "namespaces", id]
        var namespaceId = parts.length >= 6 ? decode(parts[5]) : "unknown";
        var json = "{\"id\":\"" + namespaceId + "\"}";
        sendJson(exchange, 200, json);
    }

    // ---- dashboard handlers -------------------------------------------------

    /**
     * GET /api/v1/{tenantId}/dashboards?page=&size= → search (paged)
     * GET /api/v1/{tenantId}/dashboards/{id} with Accept: application/x-yaml → YAML fetch
     */
    private void handleSearchOrGetDashboard(HttpExchange exchange, String path) throws IOException {
        exchange.getRequestBody().readAllBytes();
        var tenantId = extractTenantId(path, "dashboards");

        // Check if the path ends with a dashboard id (not just the base /dashboards path)
        var parts = path.split("/");
        // /api/v1/{tenant}/dashboards -> parts[4]="dashboards", length=5
        // /api/v1/{tenant}/dashboards/{id} -> parts[5]=id, length=6
        var isDashboardById = parts.length >= 6 && !"dashboards".equals(parts[5]);

        if (isDashboardById) {
            // Single dashboard YAML fetch (Accept: application/x-yaml)
            var dashboardId = decode(parts[5]);
            handleGetDashboardYaml(exchange, tenantId, dashboardId);
        } else {
            // Paged search
            handleSearchDashboards(exchange, tenantId);
        }
    }

    private void handleSearchDashboards(HttpExchange exchange, String tenantId) throws IOException {
        if (dashboardRepo == null) {
            sendJson(exchange, 200, "{\"results\":[],\"total\":0}");
            return;
        }

        var dashboards = dashboardRepo.findAll(tenantId);
        var sb = new StringBuilder("{\"results\":[");
        for (int i = 0; i < dashboards.size(); i++) {
            var d = dashboards.get(i);
            if (i > 0) sb.append(",");
            // Include a non-null updated timestamp so production code can compare revisions
            var updatedTs = d.getUpdated() != null ? d.getUpdated().toString() : "1970-01-01T00:00:00Z";
            sb.append("{\"id\":\"").append(d.getId())
              .append("\",\"title\":\"").append(d.getId())
              .append("\",\"updated\":\"").append(updatedTs).append("\"}");
        }
        sb.append("],\"total\":").append(dashboards.size()).append("}");
        sendJson(exchange, 200, sb.toString());
    }

    private void handleGetDashboardYaml(HttpExchange exchange, String tenantId, String dashboardId) throws IOException {
        if (dashboardRepo == null) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
            return;
        }

        var dashboards = dashboardRepo.findAll(tenantId);
        var match = dashboards.stream().filter(d -> dashboardId.equals(d.getId())).findFirst();
        if (match.isPresent() && match.get().getSourceCode() != null) {
            var yaml = match.get().getSourceCode().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/x-yaml");
            exchange.sendResponseHeaders(200, yaml.length);
            exchange.getResponseBody().write(yaml);
        } else if (match.isPresent()) {
            // If no source code in memory, generate minimal YAML with a non-null updated timestamp
            var updated = match.get().getUpdated() != null ? match.get().getUpdated().toString() : "1970-01-01T00:00:00Z";
            var yaml = ("id: " + dashboardId + "\ntitle: " + dashboardId + "\nupdated: " + updated + "\n").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/x-yaml");
            exchange.sendResponseHeaders(200, yaml.length);
            exchange.getResponseBody().write(yaml);
        } else {
            exchange.sendResponseHeaders(404, -1);
        }
        exchange.close();
    }

    /**
     * POST /api/v1/{tenantId}/dashboards — create or update dashboard.
     */
    private void handleCreateDashboard(HttpExchange exchange, String path) throws IOException {
        var tenantId = extractTenantId(path, "dashboards");
        var yamlBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);

        if (dashboardRepo != null && !yamlBody.isBlank()) {
            try {
                var parsed = YamlParser.parse(yamlBody, Dashboard.class).toBuilder()
                    .tenantId(tenantId)
                    .sourceCode(yamlBody)
                    .build();
                dashboardRepo.save(parsed, yamlBody);
            } catch (Exception e) {
                // log and ignore parse errors
            }
        }

        sendJson(exchange, 200, "{\"id\":\"created\"}");
    }

    /**
     * DELETE /api/v1/{tenantId}/dashboards/{id}
     */
    private void handleDeleteDashboard(HttpExchange exchange, String path) throws IOException {
        exchange.getRequestBody().readAllBytes();
        if (dashboardRepo != null) {
            var parts = path.split("/");
            if (parts.length >= 6) {
                var tenantId = decode(parts[3]);
                var dashboardId = decode(parts[5]);
                dashboardRepo.delete(tenantId, dashboardId);
            }
        }
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    // ---- utility ------------------------------------------------------------

    /** Extracts tenantId from a path like /api/v1/{tenant}/flows/... */
    private static String extractTenantId(String path, String resourceSegment) {
        // Path: /api/v1/{tenant}/{resourceSegment}/...
        var parts = path.split("/");
        // parts[0]="" [1]="api" [2]="v1" [3]=tenant [4]=resource
        return parts.length >= 4 ? decode(parts[3]) : TenantService.MAIN_TENANT;
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

    /**
     * Imports a YAML flow into the in-memory repository.
     * Handles both create and update via {@link FlowRepositoryInterface#create}.
     */
    private void importYaml(String tenantId, String yaml) {
        try {
            var flow = YamlParser.parse(yaml, Flow.class);
            var existing = flowRepo.findByNamespaceWithSource(tenantId, flow.getNamespace())
                .stream()
                .filter(f -> f.getId().equals(flow.getId()))
                .findFirst();

            if (existing.isPresent()) {
                // Update: create a new revision
                var updated = GenericFlow.fromYaml(tenantId, yaml);
                flowRepo.update(updated.toBuilder().source(yaml.stripTrailing()).build(), existing.get());
            } else {
                var created = GenericFlow.fromYaml(tenantId, yaml);
                flowRepo.create(created.toBuilder().source(yaml.stripTrailing()).build());
            }
        } catch (Exception e) {
            // Invalid YAML — silently skip (validated separately)
        }
    }

    /**
     * Extracts YAML content from a multipart/form-data body.
     * Falls back to treating the whole body as YAML if parsing fails.
     */
    private static String extractYamlFromMultipart(String contentType, byte[] body) {
        if (contentType == null) {
            return new String(body, StandardCharsets.UTF_8);
        }

        // Try to extract boundary
        var m = MULTIPART_BOUNDARY_PATTERN.matcher(contentType);
        if (!m.find()) {
            return new String(body, StandardCharsets.UTF_8);
        }

        var boundary = "--" + m.group(1).trim();
        var bodyStr = new String(body, StandardCharsets.UTF_8);

        // Find the YAML part after the Content-Disposition / Content-Type headers
        // Look for the section after the blank line that follows the headers
        int yamlStart = -1;
        var lines = bodyStr.split("\r\n|\n");
        boolean inPart = false;
        boolean pastHeaders = false;
        var yamlBuilder = new StringBuilder();

        for (var line : lines) {
            if (line.startsWith(boundary)) {
                if (inPart && pastHeaders && !yamlBuilder.isEmpty()) {
                    // We already collected content — stop at next boundary
                    break;
                }
                inPart = true;
                pastHeaders = false;
                yamlBuilder.setLength(0);
                continue;
            }
            if (!inPart) continue;
            if (!pastHeaders) {
                if (line.isBlank()) {
                    pastHeaders = true;
                }
                continue;
            }
            // Collect the content lines
            if (line.startsWith(boundary.substring(0, 2) + boundary.substring(2).replaceAll("-+$", ""))) {
                break; // closing boundary
            }
            yamlBuilder.append(line).append("\n");
        }

        var result = yamlBuilder.toString().strip();
        return result.isBlank() ? bodyStr : result;
    }
}
