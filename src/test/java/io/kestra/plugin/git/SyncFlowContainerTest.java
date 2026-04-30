package io.kestra.plugin.git;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.sdk.model.Flow;
import io.micronaut.context.annotation.Value;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration test for {@link SyncFlow} against a live Kestra container.
 * Complements (does not replace) the unit-level {@link SyncFlowTest} mock-server tests.
 *
 * <p>The Kestra container is started once per class by {@link AbstractKestraContainerTest}.
 * Kestra API credentials are hardcoded for test use only.
 * GitHub credentials are read from the {@code kestra.git.pat} Micronaut property (set via {@code GH_PERSONAL_TOKEN}).
 */
@KestraTest
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.username", value = "admin@admin.com")
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.password", value = "Root!1234")
public class SyncFlowContainerTest extends AbstractKestraContainerTest {

    private static final String TARGET_NAMESPACE = "io.kestra.tests.container.syncflow";
    private static final String REPO_URL = "https://github.com/kestra-io/unit-tests";
    private static final String BRANCH = "sync";
    private static final String FLOW_PATH = "to_clone/_flows/first-flow.yml";

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.git.pat}")
    private String gitPat;

    @Test
    void syncFlowFromGit_shouldLandFlowInKestra() throws Exception {
        var runContext = buildRunContext();

        var task = SyncFlow.builder()
            .url(Property.ofValue(REPO_URL))
            .username(Property.ofValue(gitPat))
            .password(Property.ofValue(gitPat))
            .branch(Property.ofValue(BRANCH))
            .flowPath(Property.ofValue(FLOW_PATH))
            .targetNamespace(Property.ofValue(TARGET_NAMESPACE))
            .kestraUrl(Property.ofValue(kestraUrl))
            .build();

        SyncFlow.Output output = task.run(runContext);

        assertThat("output must be present", output, notNullValue());
        assertThat("flowId must be present", output.getFlowId(), notNullValue());
        assertThat("namespace must match target", output.getNamespace(), is(TARGET_NAMESPACE));
        assertThat("revision must be present", output.getRevision(), notNullValue());

        List<Flow> flows = kestraTestDataUtils.listFlowsByNamespace(TARGET_NAMESPACE, TENANT_ID);
        assertThat("at least one flow must have been synced to the target namespace", flows, not(empty()));
    }

    private RunContext buildRunContext() {
        var rc = runContextFactory.of(
            Map.of(
                "flow", Map.of(
                    "tenantId", "main",
                    "namespace", TARGET_NAMESPACE,
                    "id", "sync-flow-container-test"
                )
            )
        );
        runContextFactory.initializer().forExecutor((DefaultRunContext) rc);
        return rc;
    }
}
