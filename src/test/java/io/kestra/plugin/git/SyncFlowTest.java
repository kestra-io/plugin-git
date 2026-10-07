package io.kestra.plugin.git;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.git.shared.AbstractKestraTask;
import io.kestra.plugin.git.shared.testkit.AbstractGitTest;
import io.kestra.plugin.git.shared.testkit.MockKestraApiServer;

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

    @Inject
    private FlowRepositoryInterface flowRepository;

    /**
     * Regression test for https://github.com/kestra-io/plugin-git/issues/335: a non-404 failure on the
     * single-flow lookup used to fall through to `projectedRevision = 1`, fabricating a revision for what
     * might be an existing flow. Dry-run must now abort with an actionable error instead. Driven through the
     * shared {@link MockKestraApiServer} harness so the whole task path is exercised, not a hand-rolled server.
     */
    @ParameterizedTest
    @ValueSource(ints = {401, 403, 500})
    void dryRun_flowLookupFailsWithNon404Status_shouldThrowInsteadOfFabricatingRevision(int status) throws Exception {
        try (MockKestraApiServer server = MockKestraApiServer.start(flowRepository)) {
            server.forceGetFlowStatus(TARGET_NAMESPACE, "first-flow", status);

            SyncFlow task = SyncFlow.builder()
                .url(Property.ofExpression("{{url}}"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .branch(Property.ofExpression("{{branch}}"))
                .targetNamespace(Property.ofValue(TARGET_NAMESPACE))
                .flowPath(Property.ofValue("to_clone/_flows/first-flow.yml"))
                .dryRun(Property.ofValue(true))
                .kestraUrl(Property.ofValue(server.url()))
                .auth(
                    AbstractKestraTask.Auth.builder()
                        .username(Property.ofValue("user"))
                        .password(Property.ofValue("pass"))
                        .build()
                )
                .build();

            Exception exception = assertThrows(
                io.kestra.core.exceptions.KestraRuntimeException.class,
                () -> task.run(runContext())
            );
            assertThat(exception.getMessage(), containsString(TARGET_NAMESPACE));
            assertThat(exception.getMessage(), containsString("first-flow"));
            assertThat(exception.getMessage(), containsString(String.valueOf(status)));
        }
    }

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
                .auth(
                    AbstractKestraTask.Auth.builder()
                        .username(Property.ofValue("user"))
                        .password(Property.ofValue("pass"))
                        .build()
                )
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
                .auth(
                    AbstractKestraTask.Auth.builder()
                        .username(Property.ofValue("user"))
                        .password(Property.ofValue("pass"))
                        .build()
                )
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
                .auth(
                    AbstractKestraTask.Auth.builder()
                        .username(Property.ofValue("user"))
                        .password(Property.ofValue("pass"))
                        .build()
                )
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
                .auth(
                    AbstractKestraTask.Auth.builder()
                        .username(Property.ofValue("user"))
                        .password(Property.ofValue("pass"))
                        .build()
                )
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

    @Test
    void missingBranch_shouldFailInsteadOfFallingBackToDefaultBranch() throws Exception {
        // if the fallback bug regresses, the task would clone the default branch and hit this endpoint
        var importCallCount = new java.util.concurrent.atomic.AtomicInteger(0);
        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/" + TENANT_ID + "/flows/import", exchange ->
        {
            importCallCount.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();

        try {
            var kestraUrl = "http://localhost:" + server.getAddress().getPort();
            SyncFlow task = SyncFlow.builder()
                .url(Property.ofExpression("{{url}}"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .branch(Property.ofValue("does-not-exist-on-remote"))
                .targetNamespace(Property.ofValue(TARGET_NAMESPACE))
                .flowPath(Property.ofValue("to_clone/_flows/first-flow.yml"))
                .kestraUrl(Property.ofValue(kestraUrl))
                .auth(
                    AbstractKestraTask.Auth.builder()
                        .username(Property.ofValue("user"))
                        .password(Property.ofValue("pass"))
                        .build()
                )
                .build();

            assertThrows(
                IllegalArgumentException.class,
                () -> task.run(runContext())
            );
            assertThat("no flow should have been imported into the target namespace", importCallCount.get(), is(0));
        } finally {
            server.stop(0);
        }
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
