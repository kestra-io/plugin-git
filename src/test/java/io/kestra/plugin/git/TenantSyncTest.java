package io.kestra.plugin.git;

import com.sun.net.httpserver.HttpServer;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.tenant.TenantService;
import io.kestra.sdk.KestraClient;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
                type: io.kestra.core.tasks.log.Log
                message: hello
            """;
        var exportedZip = zippedYaml("my.namespace/exported-flow.yaml", yaml);
        var validateCalls = new AtomicInteger();

        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/" + TENANT_ID + "/flows/export/by-query", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, exportedZip.length);
            exchange.getResponseBody().write(exportedZip);
            exchange.close();
        });
        server.createContext("/api/v1/" + TENANT_ID + "/flows/validate", exchange -> {
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
            var runContext = runContextFactory.of(Map.of(
                "flow", Map.of(
                    "tenantId", TENANT_ID,
                    "namespace", NAMESPACE,
                    "id", "tenant-sync-test"
                )
            ));
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
            io.kestra.core.runners.RunContext.class,
            String.class,
            TenantSync.OnInvalidSyntax.class
        );
        method.setAccessible(true);
        return method;
    }
}
