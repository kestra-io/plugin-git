package io.kestra.plugin.git;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.git.shared.AbstractGitTask;
import io.kestra.plugin.git.shared.SourceOfTruth;
import io.kestra.plugin.git.shared.SourceOfTruthOverrides;
import io.kestra.sdk.KestraClient;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class TenantSyncTest {
    private static final String TENANT_ID = TenantService.MAIN_TENANT;
    private static final String NAMESPACE = "my.namespace";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldFailOnInvalidTaskDefinitionFromKestraExport() throws Exception {
        var yaml = """
            id: exported-flow
            namespace: my.namespace

            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: hello
            """;
        var exportedZip = zippedYaml("my.namespace/exported-flow.yaml", yaml);
        var validateCalls = new AtomicInteger();

        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/" + TENANT_ID + "/flows/export/by-query", exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, exportedZip.length);
            exchange.getResponseBody().write(exportedZip);
            exchange.close();
        });
        server.createContext("/api/v1/" + TENANT_ID + "/flows/validate", exchange ->
        {
            validateCalls.incrementAndGet();
            exchange.getRequestBody().readAllBytes();

            var body = """
                [
                  {
                    "index": 0,
                    "constraints": "invalid task definition"
                  }
                ]
                """;
            var payload = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });
        server.start();

        try {
            var task = TenantSync.builder().build();
            var method = fetchFlowsMethod();
            var runContext = runContextFactory.of(
                Map.of(
                    "flow", Map.of(
                        "tenantId", TENANT_ID,
                        "namespace", NAMESPACE,
                        "id", "tenant-sync-test"
                    )
                )
            );
            var kestraClient = KestraClient.builder()
                .url("http://localhost:" + server.getAddress().getPort())
                .basicAuth("user", "pass")
                .build();

            var exception = assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(task, kestraClient, runContext, NAMESPACE, TenantSync.OnInvalidSyntax.FAIL)
            );
            assertThat(exception.getCause(), instanceOf(KestraRuntimeException.class));
            assertThat(exception.getCause().getMessage(), containsString("FLOW from entry my.namespace/exported-flow.yaml"));
            assertThat(exception.getCause().getMessage(), containsString("invalid task definition"));
            assertEquals(1, validateCalls.get());
        } finally {
            server.stop(0);
        }
    }

    private static byte[] zippedYaml(String entryName, String yaml) throws Exception {
        var output = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(output)) {
            zip.putNextEntry(new ZipEntry(entryName));
            zip.write(yaml.getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        return output.toByteArray();
    }

    private static Method fetchFlowsMethod() throws Exception {
        var method = TenantSync.class.getDeclaredMethod(
            "fetchFlowsFromKestra",
            KestraClient.class,
            RunContext.class,
            String.class,
            TenantSync.OnInvalidSyntax.class
        );
        method.setAccessible(true);
        return method;
    }

    @Test
    void shouldReadNamespaceFilesFromSymlinkedDirectories(@TempDir Path tempDir) throws Exception {
        Path filesDir = Files.createDirectories(tempDir.resolve("my.namespace").resolve("files"));
        Path externalDir = Files.createDirectories(tempDir.resolve("external"));
        Files.writeString(externalDir.resolve("script.py"), "print('hello')", StandardCharsets.UTF_8);

        Path symlink = filesDir.resolve("linked");
        try {
            Files.createSymbolicLink(symlink, externalDir);
        } catch (UnsupportedOperationException | SecurityException | FileSystemException ignored) {
        }

        var task = TenantSync.builder().build();
        var method = fetchReadGitFilesMethod();

        @SuppressWarnings("unchecked")
        Map<String, byte[]> gitFiles = (Map<String, byte[]>) method.invoke(task, filesDir);

        assertEquals("print('hello')", new String(gitFiles.get("linked/script.py"), StandardCharsets.UTF_8));
    }

    private static Method fetchReadGitFilesMethod() throws Exception {
        var method = TenantSync.class.getDeclaredMethod(
            "readGitFiles",
            Path.class
        );
        method.setAccessible(true);
        return method;
    }

    @Test
    void shouldIgnoreGlobalDashboardsDirectoryWhenDiscoveringNamespaces(@TempDir Path tempDir) throws Exception {
        Files.createDirectories(tempDir.resolve("my.namespace").resolve("flows"));
        Path dashboardsDir = Files.createDirectories(tempDir.resolve("_global").resolve("dashboards"));
        Files.writeString(dashboardsDir.resolve("my-dashboard.yaml"), "id: my-dashboard\n", StandardCharsets.UTF_8);

        var task = TenantSync.builder().build();
        var method = TenantSync.class.getDeclaredMethod("discoverGitNamespaces", Path.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Set<String> namespaces = (Set<String>) method.invoke(task, tempDir);

        // Dashboards moved to EE, so `_global/dashboards` is no longer a synced path and must not fail the sync
        assertEquals(Set.of("my.namespace"), namespaces);
    }

    @Test
    void sourceOfTruthOverrides_resolvesPerKindWithFallback() throws Exception {
        RunContext runContext = runContextFactory.of();

        assertEquals(SourceOfTruth.KESTRA, SourceOfTruthOverrides.resolve(runContext, null, SourceOfTruth.KESTRA));
        assertEquals(
            SourceOfTruth.GIT,
            SourceOfTruthOverrides.resolve(runContext, Property.ofValue(SourceOfTruth.GIT), SourceOfTruth.KESTRA)
        );
    }

    @Test
    void gitHasContent_narrowsProbeToResolvedSourcePerKind(@TempDir Path tempDir) throws Exception {
        Path namespaceRoot = Files.createDirectories(tempDir.resolve("my.namespace"));
        Files.createDirectories(namespaceRoot.resolve("files"));

        // gitHasContent now lives on the shared AbstractGitTask base (reused by NamespaceSync too), not on TenantSync itself
        var method = AbstractGitTask.class.getDeclaredMethod(
            "gitHasContent", Path.class, SourceOfTruth.class, SourceOfTruth.class
        );
        method.setAccessible(true);

        // namespaceFiles is GIT-sourced and files/ exists -> counts, even though flows/ is missing entirely
        assertEquals(true, method.invoke(null, namespaceRoot, SourceOfTruth.KESTRA, SourceOfTruth.GIT));

        // flows is GIT-sourced but flows/ doesn't exist; files/ exists but stays KESTRA-sourced -> must not false-positive
        assertEquals(false, method.invoke(null, namespaceRoot, SourceOfTruth.GIT, SourceOfTruth.KESTRA));
    }

    @Test
    void mixedDirections_recordsBothAPushAndAPullDiffInOneRun(@TempDir Path tempDir) throws Exception {
        var task = TenantSync.builder().build();
        var runContext = runContextFactory.of(
            Map.of(
                "flow", Map.of(
                    "tenantId", TENANT_ID,
                    "namespace", NAMESPACE,
                    "id", "tenant-sync-mixed-test"
                )
            )
        );

        var flowYaml = """
            id: pushed-flow
            namespace: my.namespace

            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: hello
            """;
        var kestraFlows = List.of(FlowWithSource.of(YamlParser.parse(flowYaml, io.kestra.core.models.flows.Flow.class), flowYaml));

        var diffs = new ArrayList<AbstractGitTask.DiffLine>();
        var apply = new ArrayList<Runnable>();

        // flows: Kestra is the source of truth -> a Kestra-only flow is pushed to Git
        planFlowsMethod().invoke(
            task, null, runContext, tempDir.resolve("flows"),
            Map.of(), kestraFlows, NAMESPACE,
            SourceOfTruth.KESTRA, TenantSync.WhenMissingInSource.KEEP, TenantSync.OnInvalidSyntax.FAIL,
            List.of(), true, diffs, apply
        );

        // Namespace Files: Git is the source of truth -> a Git-only file is pulled into Kestra
        planNamespaceFilesMethod().invoke(
            task, runContext, null, tempDir.resolve("files"),
            Map.of("pulled.txt", "from-git".getBytes(StandardCharsets.UTF_8)), Map.of(), NAMESPACE,
            SourceOfTruth.GIT, TenantSync.WhenMissingInSource.KEEP,
            List.of(), true, diffs, apply
        );

        assertEquals(2, diffs.size());
        assertTrue(diffs.stream().anyMatch(d -> d.getKind() == AbstractGitTask.Kind.FLOW && d.getAction() == AbstractGitTask.Action.ADDED));
        assertTrue(diffs.stream().anyMatch(d -> d.getKind() == AbstractGitTask.Kind.FILE && d.getAction() == AbstractGitTask.Action.ADDED));
        assertFalse(diffs.stream().anyMatch(d -> d.getKind() == AbstractGitTask.Kind.FLOW && d.getAction() == AbstractGitTask.Action.DELETED_GIT));
    }

    private static Method planFlowsMethod() throws Exception {
        var method = TenantSync.class.getDeclaredMethod(
            "planFlows",
            KestraClient.class, RunContext.class, Path.class,
            Map.class, List.class, String.class,
            SourceOfTruth.class, TenantSync.WhenMissingInSource.class, TenantSync.OnInvalidSyntax.class,
            List.class, boolean.class, List.class, List.class
        );
        method.setAccessible(true);
        return method;
    }

    private static Method planNamespaceFilesMethod() throws Exception {
        var method = TenantSync.class.getDeclaredMethod(
            "planNamespaceFiles",
            RunContext.class, KestraClient.class, Path.class,
            Map.class, Map.class, String.class,
            SourceOfTruth.class, TenantSync.WhenMissingInSource.class,
            List.class, boolean.class, List.class, List.class
        );
        method.setAccessible(true);
        return method;
    }

}
