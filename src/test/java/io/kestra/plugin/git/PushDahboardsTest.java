package io.kestra.plugin.git;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.dashboards.Dashboard;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.DashboardRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.git.services.GitService;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.eclipse.jgit.lib.Constants.R_HEADS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class PushDahboardsTest extends AbstractGitTest {
    public static final String DESCRIPTION = "One-task push";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private YamlParser yamlFlowParser;

    @Inject
    private DashboardRepositoryInterface dashboardRepositoryInterface;

    @Test
    void defaultCase_DefaultRegex() throws Exception {
        String tenantId = "my-tenant";
        String branch = IdUtils.create();
        String gitDirectory = "my-dashboard";

        String title1 = "firstTitle" + IdUtils.create();
        String title2 = "secondTitle" + IdUtils.create();
        String dashboardId1 = "firstId" + IdUtils.create();
        String dashboardId2 = "secondId" + IdUtils.create();

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, gitDirectory);

        Dashboard createdDashboard1 = DashboardUtils.createDashboard(dashboardRepositoryInterface, yamlFlowParser, tenantId, title1, dashboardId1);
        Dashboard createdDashboard2 = DashboardUtils.createDashboard(dashboardRepositoryInterface, yamlFlowParser, tenantId, title2, dashboardId2);

        try {
            PushDashboards pushDashboards = PushDashboards.builder()
                .id(PushDahboardsTest.class.getSimpleName())
                .type(PushDashboards.class.getName())
                .branch(new Property<>("{{branch}}"))
                .url(new Property<>("{{url}}"))
                .commitMessage(new Property<>("Push from CI - {{description}}"))
                .gitDirectory(new Property<>("{{gitDirectory}}"))
                .username(new Property<>("{{pat}}"))
                .password(new Property<>("{{pat}}"))
                .authorEmail(new Property<>("{{email}}"))
                .authorName(new Property<>("{{name}}"))
                .build();

            PushDashboards.Output pushDashboardsOutput = pushDashboards.run(runContext);
            GitService gitService = new GitService(pushDashboards);
            assertThat(gitService.branchExists(runContext, branch), is(true));

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(new Property<>(repositoryUrl))
                .username(new Property<>(pat))
                .password(new Property<>(pat))
                .branch(new Property<>(branch))
                .build();

            RunContext cloneRunContext = runContextFactory.of();
            Clone.Output cloneOutput = clone.run(cloneRunContext);

            //Verify BOTH dashboard ar present
            File dashboardFile1 = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), createdDashboard1.getId() + ".yml");
            File dashboardFile2 = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), createdDashboard2.getId() + ".yml");
            assertThat(dashboardFile1.exists(), is(true));
            assertThat(dashboardFile2.exists(), is(true));

            //Verify first dashboard content
            String fileContent1 = FileUtils.readFileToString(dashboardFile1, "UTF-8");
            assertThat(fileContent1, is(createdDashboard1.getSourceCode()));
            assertThat(fileContent1, containsString("id: " + dashboardId1));
            assertThat(fileContent1, containsString("title: " + title1));

            //Verify second dashboard content
            String fileContent2 = FileUtils.readFileToString(dashboardFile2, "UTF-8");
            assertThat(fileContent2, is(createdDashboard2.getSourceCode()));
            assertThat(fileContent2, containsString("id: " + dashboardId2));
            assertThat(fileContent2, containsString("title: " + title2));

            RevCommit revCommit = assertIsLastCommit(cloneRunContext, pushDashboardsOutput);
            assertThat(revCommit.getFullMessage(), is("Push from CI - " + DESCRIPTION));
            assertAuthor(revCommit, gitUserEmail, gitUserName);
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void defaultCase_singleRegex() throws Exception {
        String tenantId = "my-tenant";
        String branch = IdUtils.create();
        String gitDirectory = "my-dashboard";

        String title1 = "firstTitle" + IdUtils.create();
        String title2 = "secondTitle" + IdUtils.create();
        String dashboardId1 = "firstId" + IdUtils.create();
        String dashboardId2 = "secondId" + IdUtils.create();

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, gitDirectory);

        Dashboard createdDashboard1 = DashboardUtils.createDashboard(dashboardRepositoryInterface, yamlFlowParser, tenantId, title1, dashboardId1);
        Dashboard createdDashboard2 = DashboardUtils.createDashboard(dashboardRepositoryInterface, yamlFlowParser, tenantId, title2, dashboardId2);

        try {
            PushDashboards pushDashboards = PushDashboards.builder()
                .id(PushDahboardsTest.class.getSimpleName())
                .type(PushDashboards.class.getName())
                .branch(new Property<>("{{branch}}"))
                .url(new Property<>("{{url}}"))
                .commitMessage(new Property<>("Push from CI - {{description}}"))
                .gitDirectory(new Property<>("{{gitDirectory}}"))
                .username(new Property<>("{{pat}}"))
                .password(new Property<>("{{pat}}"))
                .dashboards("first*")
                .authorEmail(new Property<>("{{email}}"))
                .authorName(new Property<>("{{name}}"))
                .build();

            pushDashboards.run(runContext);
            GitService gitService = new GitService(pushDashboards);
            assertThat(gitService.branchExists(runContext, branch), is(true));

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(new Property<>(repositoryUrl))
                .username(new Property<>(pat))
                .password(new Property<>(pat))
                .branch(new Property<>(branch))
                .build();

            RunContext cloneRunContext = runContextFactory.of();
            Clone.Output cloneOutput = clone.run(cloneRunContext);

            File dashboardFile1 = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), createdDashboard1.getId() + ".yml");
            assertThat(dashboardFile1.exists(), is(true));

            File dashboardFile2 = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), createdDashboard2.getId() + ".yml");
            assertThat(dashboardFile2.exists(), is(false));


        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    private RunContext runContext(String tenantId, String repositoryUrl, String gitUserEmail, String gitUserName, String branch, String gitDirectory) {
        return runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, gitDirectory, null, false);
    }

    private RunContext runContext(String tenantId, String url, String authorEmail, String authorName, String branch, String gitDirectory, List<String> flows, boolean useStringPebbleArray) {
        Map<String, Object> map = new HashMap<>(Map.of(
            "flow", Map.of(
                "tenantId", tenantId,
                "namespace", "system"
            ),
            "url", url,
            "description", DESCRIPTION,
            "pat", pat,
            "email", authorEmail,
            "name", authorName,
            "branch", branch,
            "gitDirectory", gitDirectory
        ));

        if (flows != null && !flows.isEmpty()) {
            if (useStringPebbleArray) {
                map.put("flows",  flows);
            } else {
                for (int i=0; i<flows.size(); i++) {
                    map.put("flow" + (i + 1), flows.get(i));
                }
            }
        }
        return runContextFactory.of(map);
    }

    private static RevCommit assertIsLastCommit(RunContext cloneRunContext, PushDashboards.Output pushOutput) throws IOException, GitAPIException {
        RevCommit revCommit;
        try(Git git = Git.open(cloneRunContext.workingDir().path().toFile())) {
            revCommit = StreamSupport.stream(git.log().setMaxCount(1).call().spliterator(), false).findFirst().orElse(null);
        }
        assertThat(revCommit.getId().getName(), is(pushOutput.getCommitId()));

        return revCommit;
    }

    private static void assertAuthor(RevCommit revCommit, String authorEmail, String authorName) {
        assertThat(revCommit.getAuthorIdent().getEmailAddress(), is(authorEmail));
        assertThat(revCommit.getAuthorIdent().getName(), is(authorName));
    }

    private void deleteRemoteBranch(Path gitDirectory, String branchName) throws GitAPIException, IOException {
        try(Git git = Git.open(gitDirectory.toFile())) {
            git.checkout().setName("tmp").setCreateBranch(true).call();
            git.branchDelete().setBranchNames(R_HEADS + branchName).call();
            RefSpec refSpec = new RefSpec()
                .setSource(null)
                .setDestination(R_HEADS + branchName);
            git.push().setCredentialsProvider(new UsernamePasswordCredentialsProvider(pat, pat)).setRefSpecs(refSpec).setRemote("origin").call();
        }
    }
}
