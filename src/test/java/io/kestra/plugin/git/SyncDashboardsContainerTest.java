package io.kestra.plugin.git;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration test for {@link SyncDashboards} against a live Kestra container.
 * Complements (does not replace) the unit-level {@link SyncDashboardsTest} mock-server tests.
 *
 * <p>The Kestra container is started once per class by {@link AbstractKestraContainerTest}.
 * Kestra API credentials are hardcoded for test use only.
 * GitHub credentials are read from the {@code kestra.git.pat} Micronaut property (set via {@code GH_PERSONAL_TOKEN}).
 */
@KestraTest
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.username", value = "admin@admin.com")
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.password", value = "Root!1234")
public class SyncDashboardsContainerTest extends AbstractKestraContainerTest {

    private static final String TARGET_NAMESPACE = "io.kestra.tests.container.syncdashboards";
    private static final String REPO_URL = "https://github.com/kestra-io/unit-tests";
    private static final String BRANCH = "sync-dashboards";
    private static final String GIT_DIRECTORY = "to_clone/_dashboards";

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.git.pat}")
    private String gitPat;

    @Test
    void syncDashboardsFromGit_shouldProduceDiffOutput() throws Exception {
        var runContext = buildRunContext();

        var task = SyncDashboards.builder()
            .url(Property.ofValue(REPO_URL))
            .username(Property.ofValue(gitPat))
            .password(Property.ofValue(gitPat))
            .branch(Property.ofValue(BRANCH))
            .gitDirectory(Property.ofValue(GIT_DIRECTORY))
            .kestraUrl(Property.ofValue(kestraUrl))
            .build();

        SyncDashboards.Output output = task.run(runContext);

        assertThat("output must be present", output, notNullValue());
        assertThat("dashboards diff URI must be present", output.getDashboards(), notNullValue());
    }

    private RunContext buildRunContext() {
        var rc = runContextFactory.of(
            Map.of(
                "flow", Map.of(
                    "tenantId", "main",
                    "namespace", TARGET_NAMESPACE,
                    "id", "sync-dashboards-container-test"
                )
            )
        );
        runContextFactory.initializer().forExecutor((DefaultRunContext) rc);
        return rc;
    }
}
