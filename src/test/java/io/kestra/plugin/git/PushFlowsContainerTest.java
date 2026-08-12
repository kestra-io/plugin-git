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
 * Integration test for {@link PushFlows} against a live Kestra container and a real GitHub repository.
 * Complements (does not replace) the unit-level {@link PushFlowsTest} mock-server tests.
 *
 * <p>The Kestra container is started once per class by {@link AbstractKestraContainerTest}.
 * Kestra API credentials are hardcoded for test use only.
 * GitHub credentials are read from the {@code kestra.git.pat} Micronaut property (set via {@code GH_PERSONAL_TOKEN}).
 * The remote branch is deleted after each test run to avoid orphan branches.
 */
@KestraTest
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.username", value = "admin@admin.com")
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.password", value = "Root!1234")
public class PushFlowsContainerTest extends AbstractKestraContainerTest {

    private static final String REPO_URL = "https://github.com/kestra-io/unit-tests";
    private static final String GIT_DIRECTORY = "my-flows";

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.git.pat}")
    private String gitPat;

    @Test
    void pushFlowsToGit_shouldProduceCommitUrl() throws Exception {
        var sourceNamespace = "io.kestra.tests.container.pushflows." + IdUtils.create().toLowerCase();
        var branch = IdUtils.create();
        var runContext = buildRunContext(sourceNamespace);

        var flowYaml = """
            id: push-flows-container-test-flow
            namespace: \
            """ + sourceNamespace + """

            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: Hello from container test
            """;

        kestraTestDataUtils.createFlow(TENANT_ID, flowYaml);

        var task = PushFlows.builder()
            .url(Property.ofValue(REPO_URL))
            .username(Property.ofValue(gitPat))
            .password(Property.ofValue(gitPat))
            .branch(Property.ofValue(branch))
            .gitDirectory(Property.ofValue(GIT_DIRECTORY))
            .sourceNamespace(Property.ofValue(sourceNamespace))
            .kestraUrl(Property.ofValue(kestraUrl))
            .build();

        PushFlows.Output output = task.run(runContext);

        try {
            assertThat("output must be present", output, notNullValue());
            assertThat("commitURL must be present", output.getCommitURL(), notNullValue());
        } finally {
            // Only delete the remote branch if a git repo was actually cloned (i.e., the push succeeded at least partially)
            var gitDir = runContext.workingDir().path().resolve(".git");
            if (gitDir.toFile().exists()) {
                deleteRemoteBranch(runContext, branch);
            }
        }
    }

    private RunContext buildRunContext(String sourceNamespace) {
        var rc = runContextFactory.of(
            Map.of(
                "flow", Map.of(
                    "tenantId", "main",
                    "namespace", sourceNamespace,
                    "id", "push-flows-container-test"
                )
            )
        );
        runContextFactory.initializer().forExecutor((DefaultRunContext) rc);
        return rc;
    }

    /**
     * Deletes the remote branch created during the test to avoid leaving orphan branches in the repository.
     * Mirrors the pattern used in {@link PushFlowsTest}.
     */
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
