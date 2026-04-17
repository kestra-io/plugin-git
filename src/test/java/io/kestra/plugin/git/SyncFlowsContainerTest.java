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
 * Integration test for {@link SyncFlows} against a live Kestra container.
 * Complements (does not replace) the {@link SyncFlowsTest} mock-server tests.
 *
 * <p>The Kestra container is started once per class by {@link AbstractKestraContainerTest}.
 * Kestra API credentials ({@code admin@admin.com} / {@code Root!1234}) are hardcoded for test use only.
 * GitHub credentials are read from the {@code kestra.git.pat} Micronaut property (set via {@code GH_PERSONAL_TOKEN}).
 */
@KestraTest
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.username", value = "admin@admin.com")
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.password", value = "Root!1234")
public class SyncFlowsContainerTest extends AbstractKestraContainerTest {

    private static final String TARGET_NAMESPACE = "io.kestra.tests.container";
    private static final String REPO_URL = "https://github.com/kestra-io/unit-tests";
    private static final String BRANCH = "sync";
    private static final String GIT_DIRECTORY = "to_clone/_flows";

    @Inject
    private RunContextFactory runContextFactory;

    // Same PAT used by existing SyncFlowsTest — required for jGit to clone even public repos
    @Value("${kestra.git.pat}")
    private String gitPat;

    @Test
    void syncFlowsFromGit_shouldLandFlowsInKestra() throws Exception {
        var runContext = buildRunContext();

        var task = SyncFlows.builder()
            .url(Property.ofValue(REPO_URL))
            .username(Property.ofValue(gitPat))
            .password(Property.ofValue(gitPat))
            .branch(Property.ofValue(BRANCH))
            .gitDirectory(Property.ofValue(GIT_DIRECTORY))
            .targetNamespace(Property.ofValue(TARGET_NAMESPACE))
            .includeChildNamespaces(Property.ofValue(false))
            .delete(Property.ofValue(false))
            .kestraUrl(Property.ofValue(kestraUrl))
            .build();

        SyncFlows.Output output = task.run(runContext);

        assertThat("diff file URI must be present", output.getFlows(), notNullValue());

        List<Flow> flows = kestraTestDataUtils.listFlowsByNamespace(TARGET_NAMESPACE, TENANT_ID);
        assertThat("at least one flow must have been synced to the target namespace", flows, not(empty()));
    }

    private RunContext buildRunContext() {
        var rc = runContextFactory.of(
            Map.of(
                "flow", Map.of(
                    "tenantId", "main",
                    "namespace", TARGET_NAMESPACE,
                    "id", "sync-flows-container-test"
                )
            )
        );
        runContextFactory.initializer().forExecutor((DefaultRunContext) rc);
        return rc;
    }
}
