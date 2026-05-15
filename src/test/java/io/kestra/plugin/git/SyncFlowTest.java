package io.kestra.plugin.git;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.tenant.TenantService;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class SyncFlowTest extends AbstractGitTest {
    public static final String BRANCH = "sync";
    public static final String TENANT_ID = TenantService.MAIN_TENANT;
    public static final String TARGET_NAMESPACE = "io.kestra.synced";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void createNewFlow() throws Exception {
        var importResponse = "[\"first-flow\"]".getBytes(StandardCharsets.UTF_8);
        var flowResponse = """
            {
              "id": "first-flow",
              "namespace": "io.kestra.synced",
              "revision": 1,
              "disabled": false,
              "deleted": false,
              "tasks": []
            }
            """.getBytes(StandardCharsets.UTF_8);

        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/" + TENANT_ID + "/flows/import", exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, importResponse.length);
            exchange.getResponseBody().write(importResponse);
            exchange.close();
        });
        server.createContext("/api/v1/" + TENANT_ID + "/flows/" + TARGET_NAMESPACE + "/first-flow", exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, flowResponse.length);
            exchange.getResponseBody().write(flowResponse);
            exchange.close();
        });
        server.start();

        try {
            var kestraUrl = "http://localhost:" + server.getAddress().getPort();
            SyncFlow task = SyncFlow.builder()
                .url(Property.ofExpression("{{url}}"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .branch(Property.ofExpression("{{branch}}"))
                .targetNamespace(Property.ofValue(TARGET_NAMESPACE))
                .flowPath(Property.ofValue("to_clone/_flows/first-flow.yml"))
                .kestraUrl(Property.ofValue(kestraUrl))
                .auth(AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue("user"))
                    .password(Property.ofValue("pass"))
                    .build())
                .build();

            SyncFlow.Output output = task.run(runContext());

            assertThat(output.getFlowId(), is("first-flow"));
            assertThat(output.getNamespace(), is(TARGET_NAMESPACE));
            assertThat(output.getRevision(), is(1));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void updateExistingFlow() throws Exception {
        var importResponse = "[\"second-flow\"]".getBytes(StandardCharsets.UTF_8);
        var flowResponse = """
            {
              "id": "second-flow",
              "namespace": "io.kestra.synced",
              "revision": 2,
              "disabled": false,
              "deleted": false,
              "tasks": []
            }
            """.getBytes(StandardCharsets.UTF_8);

        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/" + TENANT_ID + "/flows/import", exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, importResponse.length);
            exchange.getResponseBody().write(importResponse);
            exchange.close();
        });
        server.createContext("/api/v1/" + TENANT_ID + "/flows/" + TARGET_NAMESPACE + "/second-flow", exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, flowResponse.length);
            exchange.getResponseBody().write(flowResponse);
            exchange.close();
        });
        server.start();

        try {
            var kestraUrl = "http://localhost:" + server.getAddress().getPort();
            SyncFlow task = SyncFlow.builder()
                .url(Property.ofExpression("{{url}}"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .branch(Property.ofExpression("{{branch}}"))
                .targetNamespace(Property.ofValue(TARGET_NAMESPACE))
                .flowPath(Property.ofValue("to_clone/_flows/second-flow.yml"))
                .kestraUrl(Property.ofValue(kestraUrl))
                .auth(AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue("user"))
                    .password(Property.ofValue("pass"))
                    .build())
                .build();

            SyncFlow.Output output = task.run(runContext());

            assertThat(output.getRevision(), is(2));
            assertThat(output.getFlowId(), is("second-flow"));
            assertThat(output.getNamespace(), is(TARGET_NAMESPACE));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void dryRun_newFlow() throws Exception {
        // validate returns empty violations (flow is valid)
        var validateResponse = "[]".getBytes(StandardCharsets.UTF_8);

        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/" + TENANT_ID + "/flows/validate", exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, validateResponse.length);
            exchange.getResponseBody().write(validateResponse);
            exchange.close();
        });
        // Return 404 for flow lookup: flow doesn't exist yet, so projected revision is 1
        server.createContext("/api/v1/" + TENANT_ID + "/flows/" + TARGET_NAMESPACE + "/first-flow", exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();

        try {
            var kestraUrl = "http://localhost:" + server.getAddress().getPort();
            SyncFlow task = SyncFlow.builder()
                .url(Property.ofExpression("{{url}}"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .branch(Property.ofExpression("{{branch}}"))
                .targetNamespace(Property.ofValue(TARGET_NAMESPACE))
                .flowPath(Property.ofValue("to_clone/_flows/first-flow.yml"))
                .dryRun(Property.ofValue(true))
                .kestraUrl(Property.ofValue(kestraUrl))
                .auth(AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue("user"))
                    .password(Property.ofValue("pass"))
                    .build())
                .build();

            SyncFlow.Output output = task.run(runContext());

            assertThat(output.getFlowId(), is("first-flow"));
            assertThat(output.getNamespace(), is(TARGET_NAMESPACE));
            // Projected revision for a new flow is 1
            assertThat(output.getRevision(), is(1));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void dryRun_existingFlow() throws Exception {
        var validateResponse = "[]".getBytes(StandardCharsets.UTF_8);
        var existingFlowResponse = """
            {
              "id": "first-flow",
              "namespace": "io.kestra.synced",
              "revision": 3,
              "disabled": false,
              "deleted": false,
              "tasks": []
            }
            """.getBytes(StandardCharsets.UTF_8);

        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/" + TENANT_ID + "/flows/validate", exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, validateResponse.length);
            exchange.getResponseBody().write(validateResponse);
            exchange.close();
        });
        server.createContext("/api/v1/" + TENANT_ID + "/flows/" + TARGET_NAMESPACE + "/first-flow", exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, existingFlowResponse.length);
            exchange.getResponseBody().write(existingFlowResponse);
            exchange.close();
        });
        server.start();

        try {
            var kestraUrl = "http://localhost:" + server.getAddress().getPort();
            SyncFlow task = SyncFlow.builder()
                .url(Property.ofExpression("{{url}}"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .branch(Property.ofExpression("{{branch}}"))
                .targetNamespace(Property.ofValue(TARGET_NAMESPACE))
                .flowPath(Property.ofValue("to_clone/_flows/first-flow.yml"))
                .dryRun(Property.ofValue(true))
                .kestraUrl(Property.ofValue(kestraUrl))
                .auth(AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue("user"))
                    .password(Property.ofValue("pass"))
                    .build())
                .build();

            SyncFlow.Output output = task.run(runContext());

            assertThat(output.getFlowId(), is("first-flow"));
            assertThat(output.getNamespace(), is(TARGET_NAMESPACE));
            // Projected revision: existing (3) + 1 = 4
            assertThat(output.getRevision(), is(4));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void fileNotFound() {
        SyncFlow task = SyncFlow.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .targetNamespace(Property.ofValue(TARGET_NAMESPACE))
            .flowPath(Property.ofValue("non_existent_file.yml"))
            // No kestraUrl needed since the task will fail before reaching the API
            .build();

        Exception exception = assertThrows(
            java.io.FileNotFoundException.class,
            () -> task.run(runContext())
        );
        assertThat(exception.getMessage(), containsString("non_existent_file.yml"));
    }

    private io.kestra.core.runners.RunContext runContext() {
        return runContextFactory.of(
            Map.of(
                "flow", Map.of(
                    "tenantId", TENANT_ID,
                    "namespace", "io.kestra.unittest",
                    "id", "test-flow"
                ),
                "url", repositoryUrl,
                "pat", pat,
                "branch", BRANCH
            )
        );
    }
}
