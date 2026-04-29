package io.kestra.plugin.git;

import java.io.IOException;
import java.util.Map;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;

import jakarta.inject.Inject;

import static org.eclipse.jgit.lib.Constants.R_HEADS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration test for {@link PushDashboards} against a live Kestra container and a real GitHub repository.
 * Validates that {@code apiClient.invokeAPI} correctly authenticates and fetches dashboard YAML
 * from a secured Kestra endpoint using credentials from the KestraClient's defaultHeaderMap.
 *
 * <p>The Kestra container is started once per class by {@link AbstractKestraContainerTest}.
 * Kestra API credentials are hardcoded for test use only.
 * GitHub credentials are read from the {@code kestra.git.pat} Micronaut property (set via {@code GH_PERSONAL_TOKEN}).
 * The remote branch is deleted after each test run to avoid orphan branches.
 */
@KestraTest
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.username", value = "admin@admin.com")
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.password", value = "Root!1234")
public class PushDashboardsContainerTest extends AbstractKestraContainerTest {

    private static final String REPO_URL = "https://github.com/kestra-io/unit-tests";
    private static final String GIT_DIRECTORY = "_dashboards";

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.git.pat}")
    private String gitPat;

    @Test
    void pushDashboardsToGit_shouldProduceCommitUrl() throws Exception {
        var branch = IdUtils.create();
        var dashboardId = "push-container-test-" + IdUtils.create().toLowerCase();
        var runContext = buildRunContext();

        var dashboardYaml = "id: " + dashboardId + "\n"
            + "title: Container Test Dashboard\n"
            + "description: Created by PushDashboardsContainerTest\n"
            + "timeWindow:\n"
            + "  default: P30D\n"
            + "  max: P365D\n"
            + "charts:\n"
            + "  - id: executions_timeseries\n"
            + "    type: io.kestra.plugin.core.dashboard.chart.TimeSeries\n"
            + "    chartOptions:\n"
            + "      displayName: Executions\n"
            + "      description: Executions duration and count per date\n"
            + "      legend:\n"
            + "        enabled: true\n"
            + "      column: date\n"
            + "      colorByColumn: state\n"
            + "    data:\n"
            + "      type: io.kestra.plugin.core.dashboard.data.Executions\n"
            + "      columns:\n"
            + "        date:\n"
            + "          field: START_DATE\n"
            + "          displayName: Date\n"
            + "        state:\n"
            + "          field: STATE\n"
            + "        total:\n"
            + "          displayName: Executions\n"
            + "          agg: COUNT\n"
            + "          graphStyle: BARS\n"
            + "        duration:\n"
            + "          displayName: Duration\n"
            + "          field: DURATION\n"
            + "          agg: SUM\n"
            + "          graphStyle: LINES\n";

        kestraTestDataUtils.createDashboard(TENANT_ID, dashboardYaml);

        var task = PushDashboards.builder()
            .url(Property.ofValue(REPO_URL))
            .username(Property.ofValue(gitPat))
            .password(Property.ofValue(gitPat))
            .branch(Property.ofValue(branch))
            .gitDirectory(Property.ofValue(GIT_DIRECTORY))
            .kestraUrl(Property.ofValue(kestraUrl))
            .build();

        PushDashboards.Output output = task.run(runContext);

        try {
            assertThat("output must be present", output, notNullValue());
            assertThat("commitURL must be present", output.getCommitURL(), notNullValue());
        } finally {
            var gitDir = runContext.workingDir().path().resolve(".git");
            if (gitDir.toFile().exists()) {
                deleteRemoteBranch(runContext, branch);
            }
        }
    }

    private RunContext buildRunContext() {
        var rc = runContextFactory.of(
            Map.of(
                "flow", Map.of(
                    "tenantId", TENANT_ID,
                    "namespace", "io.kestra.tests.container.pushdashboards",
                    "id", "push-dashboards-container-test"
                )
            )
        );
        runContextFactory.initializer().forExecutor((DefaultRunContext) rc);
        return rc;
    }

    private void deleteRemoteBranch(RunContext runContext, String branchName) throws GitAPIException, IOException {
        try (var git = Git.open(runContext.workingDir().path().toFile())) {
            git.checkout().setName("tmp").setCreateBranch(true).call();
            git.branchDelete().setBranchNames(R_HEADS + branchName).call();
            var refSpec = new RefSpec()
                .setSource(null)
                .setDestination(R_HEADS + branchName);
            git.push()
                .setCredentialsProvider(new UsernamePasswordCredentialsProvider(gitPat, gitPat))
                .setRefSpecs(refSpec)
                .setRemote("origin")
                .call();
        }
    }
}
