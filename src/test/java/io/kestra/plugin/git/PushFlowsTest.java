package io.kestra.plugin.git;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.Rethrow;
import io.kestra.plugin.git.shared.AbstractPushTask;
import io.kestra.plugin.git.shared.services.GitService;
import io.kestra.plugin.git.shared.testkit.AbstractGitTest;
import io.kestra.plugin.git.shared.testkit.MockKestraApiServer;

import jakarta.inject.Inject;

import static org.eclipse.jgit.lib.Constants.R_HEADS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class PushFlowsTest extends AbstractGitTest {
    public static final String DESCRIPTION = "One-task push";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FlowRepositoryInterface flowRepositoryInterface;

    private MockKestraApiServer server;

    @BeforeEach
    void startMockServer() throws IOException {
        server = MockKestraApiServer.start(flowRepositoryInterface);
    }

    @AfterEach
    void stopMockServer() {
        server.close();
    }

    @Test
    void defaultCase_SingleRegex() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);

        FlowWithSource createdFlow = this.createFlow(tenantId, "first-flow", sourceNamespace);
        String subNamespace = "sub-namespace";
        FlowWithSource createdSubNsFlow = this.createFlow(tenantId, "second-flow", sourceNamespace + "." + subNamespace);

        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
            .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
            .flows("second*")
            .includeChildNamespaces(Property.ofValue(true))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);
            GitService gitService = new GitService(pushFlows);
            assertThat(gitService.branchExists(runContext, branch), is(true));

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            RunContext cloneRunContext = runContextFactory.of();
            Clone.Output cloneOutput = clone.run(cloneRunContext);

            File flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), createdFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(false));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, subNamespace).toString(), createdSubNsFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(createdSubNsFlow.getSource().replace(sourceNamespace, targetNamespace)));

            assertThat(pushOutput.getCommitURL(), is(repositoryUrl + "/commit/" + pushOutput.getCommitId()));

            assertDiffs(
                runContext,
                pushOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/sub-namespace/second-flow.yml")
                )
            );

            RevCommit revCommit = assertIsLastCommit(cloneRunContext, pushOutput);
            assertThat(revCommit.getFullMessage(), is("Push from CI - " + DESCRIPTION));
            assertAuthor(revCommit, gitUserEmail, gitUserName);
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void defaultCase_SingleRegex_noTargetNamespace_noSourceNamespace() throws Exception {
        final String systemNamespace = "system";
        final String subNamespace = "sub-namespace";
        final String tenantId = TenantService.MAIN_TENANT;
        final String branch = IdUtils.create();
        final String gitDirectory = "my-flows";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, "", "", gitDirectory);

        //Create flows under the `system` namespace which is the default one for PushFlows when using unit tests
        FlowWithSource createdFlow = this.createFlow(tenantId, "first-flow", systemNamespace);
        FlowWithSource createdSubNsFlow = this.createFlow(tenantId, "second-flow", systemNamespace + "." + subNamespace);

        //PushFlows for `system` namespace, don't specify target and source
        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .flows("second*")
            .includeChildNamespaces(Property.ofValue(true))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);
            GitService gitService = new GitService(pushFlows);
            assertThat(gitService.branchExists(runContext, branch), is(true));

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            RunContext cloneRunContext = runContextFactory.of();
            Clone.Output cloneOutput = clone.run(cloneRunContext);

            File flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), createdFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(false));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, subNamespace).toString(), createdSubNsFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(createdSubNsFlow.getSource()));
            assertThat(fileContent, containsString("namespace: " + systemNamespace + "." + subNamespace));

            RevCommit revCommit = assertIsLastCommit(cloneRunContext, pushOutput);
            assertThat(revCommit.getFullMessage(), is("Push from CI - " + DESCRIPTION));
            assertAuthor(revCommit, gitUserEmail, gitUserName);
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void defaultCase_SingleRegexDryRun() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);

        this.createFlow(tenantId, "first-flow", sourceNamespace);
        String subNamespace = "sub-namespace";
        this.createFlow(tenantId, "second-flow", sourceNamespace + "." + subNamespace);

        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
            .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
            .flows("second*")
            .includeChildNamespaces(Property.ofValue(true))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .dryRun(Property.ofValue(true))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        PushFlows.Output pushOutput = pushFlows.run(runContext);

        GitService gitService = new GitService(pushFlows);
        assertThat(gitService.branchExists(runContext, branch), is(false));

        assertThat(pushOutput.getCommitURL(), nullValue());

        assertDiffs(
            runContext,
            pushOutput.diffFileUri(),
            List.of(
                Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/sub-namespace/second-flow.yml")
            )
        );
    }

    @Test
    void defaultCase_SingleRegex_DeleteScopedToRegex() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);

        FlowWithSource nonMatchingRegexKeptFlow = this.createFlow(tenantId, "first-flow", sourceNamespace);
        String subNamespace = "sub-namespace";
        FlowWithSource matchingRegexKeptFlow = this.createFlow(tenantId, "second-flow", sourceNamespace + "." + subNamespace);
        FlowWithSource deletedFlowOnSecondPush = this.createFlow(tenantId, "second-deleted-after-push", sourceNamespace);

        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
            .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
            .includeChildNamespaces(Property.ofValue(true))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            Clone.Output cloneOutput = clone.run(runContextFactory.of());

            File flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), nonMatchingRegexKeptFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(nonMatchingRegexKeptFlow.getSource().replace(sourceNamespace, targetNamespace)));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, subNamespace).toString(), matchingRegexKeptFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(matchingRegexKeptFlow.getSource().replace(sourceNamespace, targetNamespace)));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), deletedFlowOnSecondPush.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(deletedFlowOnSecondPush.getSource().replace(sourceNamespace, targetNamespace)));

            assertDiffs(
                runContext,
                pushOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + nonMatchingRegexKeptFlow.getId() + ".yml"),
                    Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/sub-namespace/" + matchingRegexKeptFlow.getId() + ".yml"),
                    Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + deletedFlowOnSecondPush.getId() + ".yml")
                )
            );

            flowRepositoryInterface.delete(deletedFlowOnSecondPush);
            pushOutput = pushFlows.toBuilder()
                .flows("second*")
                .build().run(runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory));

            cloneOutput = clone.run(runContextFactory.of());

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), nonMatchingRegexKeptFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(nonMatchingRegexKeptFlow.getSource().replace(sourceNamespace, targetNamespace)));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, subNamespace).toString(), matchingRegexKeptFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(matchingRegexKeptFlow.getSource().replace(sourceNamespace, targetNamespace)));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), deletedFlowOnSecondPush.getId() + ".yml");
            assertThat(flowFile.exists(), is(false));

            assertDiffs(
                runContext,
                pushOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+0", "deletions", "-10", "changes", "0", "file", gitDirectory + "/" + deletedFlowOnSecondPush.getId() + ".yml")
                )
            );
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void defaultCase_NoRegex() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);

        FlowWithSource createdFlow = this.createFlow(tenantId, sourceNamespace);
        String subNamespace = "sub-namespace";
        FlowWithSource createdSubNsFlow = this.createFlow(tenantId, sourceNamespace + "." + subNamespace);

        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
            .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
            .includeChildNamespaces(Property.ofValue(true))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            RunContext cloneRunContext = runContextFactory.of();
            Clone.Output cloneOutput = clone.run(cloneRunContext);

            File outOfScopeFile = new File(Path.of(cloneOutput.getDirectory(), "README.md").toString());
            assertThat(outOfScopeFile.exists(), is(true));

            File flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), createdFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(createdFlow.getSource().replace(sourceNamespace, targetNamespace)));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, subNamespace).toString(), createdSubNsFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(createdSubNsFlow.getSource().replace(sourceNamespace, targetNamespace)));

            assertThat(pushOutput.getCommitURL(), is(repositoryUrl + "/commit/" + pushOutput.getCommitId()));

            assertDiffs(
                runContext,
                pushOutput.getFlows(),
                List.of(
                    Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/some-flow.yml"),
                    Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/sub-namespace/some-flow.yml")
                )
            );

            RevCommit revCommit = assertIsLastCommit(cloneRunContext, pushOutput);
            assertThat(revCommit.getFullMessage(), is("Push from CI - " + DESCRIPTION));
            assertAuthor(revCommit, gitUserEmail, gitUserName);
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void defaultCase_MultipleRegex(boolean useStringPebbleArray) throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        RunContext runContext = runContext(
            tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory, List.of("first*", "second*"), useStringPebbleArray
        );

        FlowWithSource createdFlow = this.createFlow(tenantId, "first-flow", sourceNamespace);
        String subNamespace = "sub-namespace";
        FlowWithSource createdSubNsFlow = this.createFlow(tenantId, "second-flow", sourceNamespace + "." + subNamespace);
        FlowWithSource thirdFlow = this.createFlow(tenantId, "third-flow", sourceNamespace);

        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
            .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
            .flows(useStringPebbleArray ? "{{ flows }}" : List.of("{{ flow1 }}", "{{ flow2 }}"))
            .includeChildNamespaces(Property.ofValue(true))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            RunContext cloneRunContext = runContextFactory.of();
            Clone.Output cloneOutput = clone.run(cloneRunContext);

            File flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), createdFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(createdFlow.getSource().replace(sourceNamespace, targetNamespace)));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, subNamespace).toString(), createdSubNsFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(createdSubNsFlow.getSource().replace(sourceNamespace, targetNamespace)));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, subNamespace).toString(), thirdFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(false));

            assertThat(pushOutput.getCommitURL(), is(repositoryUrl + "/commit/" + pushOutput.getCommitId()));

            assertDiffs(
                runContext,
                pushOutput.getFlows(),
                List.of(
                    Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/first-flow.yml"),
                    Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/sub-namespace/second-flow.yml")
                )
            );

            RevCommit revCommit = assertIsLastCommit(cloneRunContext, pushOutput);
            assertThat(revCommit.getFullMessage(), is("Push from CI - " + DESCRIPTION));
            assertAuthor(revCommit, gitUserEmail, gitUserName);
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void defaultCase_NoRegexNoChildNsNoAuthorName() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, "", branch, sourceNamespace, targetNamespace, gitDirectory);

        FlowWithSource createdFlow = this.createFlow(tenantId, sourceNamespace);
        String subNamespace = "sub-namespace";
        FlowWithSource createdSubNsFlow = this.createFlow(tenantId, sourceNamespace + "." + subNamespace);

        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
            .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            RunContext cloneRunContext = runContextFactory.of();
            Clone.Output cloneOutput = clone.run(cloneRunContext);

            File flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), createdFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(createdFlow.getSource().replace(sourceNamespace, targetNamespace)));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, subNamespace).toString(), createdSubNsFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(false));

            assertDiffs(
                runContext,
                pushOutput.getFlows(),
                List.of(
                    Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/some-flow.yml")
                )
            );

            RevCommit revCommit = assertIsLastCommit(cloneRunContext, pushOutput);
            assertAuthor(revCommit, gitUserEmail, pat);
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void defaultCase_NoRegexNoAuthor() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        RunContext runContext = runContext(tenantId, repositoryUrl, "", "", branch, sourceNamespace, targetNamespace, gitDirectory);

        this.createFlow(tenantId, sourceNamespace);

        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
            .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
            .includeChildNamespaces(Property.ofValue(true))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            RunContext cloneRunContext = runContextFactory.of();
            clone.run(cloneRunContext);

            RevCommit revCommit = assertIsLastCommit(cloneRunContext, pushOutput);
            assertThat(revCommit.getAuthorIdent().getName(), notNullValue());
            assertThat(revCommit.getAuthorIdent().getEmailAddress(), notNullValue());
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void defaultCase_DeletePropertyFalse_PreservesExistingFiles() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        FlowWithSource keepFlow = this.createFlow(tenantId, "keep-flow", sourceNamespace);
        FlowWithSource deleteFromKestraFlow = this.createFlow(tenantId, "delete-from-kestra", sourceNamespace);

        try {
            RunContext runContext1 = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);

            PushFlows firstPush = PushFlows.builder()
                .id("pushFlows")
                .type(PushFlows.class.getName())
                .branch(Property.ofExpression("{{branch}}"))
                .url(Property.ofExpression("{{url}}"))
                .commitMessage(Property.ofValue("First push - both flows"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .authorEmail(Property.ofExpression("{{email}}"))
                .authorName(Property.ofExpression("{{name}}"))
                .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
                .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
                .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
                .delete(Property.ofValue(false))
                .kestraUrl(Property.ofValue(server.url()))
                .build();

            PushFlows.Output firstOutput = firstPush.run(runContext1);
            assertThat(firstOutput.getCommitURL(), notNullValue());

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            Clone.Output cloneOutput = clone.run(runContextFactory.of());
            File keepFlowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, keepFlow.getId() + ".yml").toString());
            File deleteFromKestraFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, deleteFromKestraFlow.getId() + ".yml").toString());

            assertThat("both files should exist after first push", keepFlowFile.exists() && deleteFromKestraFile.exists(), is(true));

            flowRepositoryInterface.delete(deleteFromKestraFlow);

            RunContext runContext2 = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);

            PushFlows secondPush = PushFlows.builder()
                .id("pushFlows")
                .type(PushFlows.class.getName())
                .branch(Property.ofExpression("{{branch}}"))
                .url(Property.ofExpression("{{url}}"))
                .commitMessage(Property.ofValue("Second push - delete=false"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .authorEmail(Property.ofExpression("{{email}}"))
                .authorName(Property.ofExpression("{{name}}"))
                .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
                .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
                .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
                .delete(Property.ofValue(false))
                .kestraUrl(Property.ofValue(server.url()))
                .build();

            PushFlows.Output secondOutput = secondPush.run(runContext2);

            cloneOutput = clone.run(runContextFactory.of());
            keepFlowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, keepFlow.getId() + ".yml").toString());
            deleteFromKestraFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, deleteFromKestraFlow.getId() + ".yml").toString());

            assertThat("keep file should be there", keepFlowFile.exists(), is(true));
            assertThat("file should still exist in Git after delete=false push", deleteFromKestraFile.exists(), is(true));

            RunContext runContext3 = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);

            PushFlows thirdPush = PushFlows.builder()
                .id("pushFlows")
                .type(PushFlows.class.getName())
                .branch(Property.ofExpression("{{branch}}"))
                .url(Property.ofExpression("{{url}}"))
                .commitMessage(Property.ofValue("Third push - delete=true"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .authorEmail(Property.ofExpression("{{email}}"))
                .authorName(Property.ofExpression("{{name}}"))
                .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
                .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
                .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
                .delete(Property.ofValue(true))
                .kestraUrl(Property.ofValue(server.url()))
                .build();

            PushFlows.Output thirdOutput = thirdPush.run(runContext3);
            assertThat(thirdOutput.getCommitId(), notNullValue());

            cloneOutput = clone.run(runContextFactory.of());
            keepFlowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, keepFlow.getId() + ".yml").toString());
            deleteFromKestraFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, deleteFromKestraFlow.getId() + ".yml").toString());

            assertThat("keep file should still exist", keepFlowFile.exists(), is(true));
            assertThat("file should be deleted from Git after delete=true push", deleteFromKestraFile.exists(), is(false));

        } finally {
            try {
                RunContext cleanupContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);
                this.deleteRemoteBranch(cleanupContext.workingDir().path(), branch);
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void defaultCase_ExcludesDraftFlows() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);

        FlowWithSource createdFlow = this.createFlow(tenantId, "first-flow", sourceNamespace);
        FlowWithSource draftFlow = createDraftFlow(flowRepositoryInterface, tenantId, "second-flow-draft", sourceNamespace);

        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
            .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            Clone.Output cloneOutput = clone.run(runContextFactory.of());

            File flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), createdFlow.getId() + ".yml");
            assertThat(flowFile.exists(), is(true));

            File draftFlowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), draftFlow.getId() + ".yml");
            assertThat(draftFlowFile.exists(), is(false));

            assertDiffs(
                runContext,
                pushOutput.getFlows(),
                List.of(
                    Map.of("additions", "+10", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + createdFlow.getId() + ".yml")
                )
            );
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    /**
     * Issue #345, need #2: "push all deletes without also including all changes and all new flows".
     * With {@code pushMode: DELETE_ONLY} a {@code delete: true} push stages ONLY the removal of a flow deleted from
     * Kestra; an unrelated change made to another flow must NOT ride along on the commit.
     */
    @Test
    void deleteOnly_pushesOnlyDeletion_leavesUnrelatedModificationOnBranch() throws Exception {
        runDeleteOnlyScenario(null, "Delete only", false);
    }

    /**
     * Issue #345, need #1: "push a delete of a single flow, ignoring all other". A DELETE_ONLY push
     * scoped by a glob removes only the flow(s) matching that glob; a flow that was also deleted from Kestra but
     * falls outside the glob's scope is left untouched on the branch, proving the glob is genuinely applied rather
     * than every deletion being pushed regardless of the filter.
     */
    @Test
    void deleteOnly_globScopedDelete_deletesOnlyMatchingFlow() throws Exception {
        runDeleteOnlyScenario("deleted-*", "Delete matching glob", true);
    }

    /**
     * Shared scenario for the DELETE_ONLY push-mode tests above: pushes a baseline of a kept flow and a
     * to-be-deleted flow, makes an unrelated pending change to the kept flow in Kestra, deletes the target flow from
     * Kestra, then runs a DELETE_ONLY push scoped by {@code flowsFilter} (or unscoped, i.e. all flows, when
     * {@code null}). Asserts the targeted flow is removed from the branch and the kept flow keeps its original
     * (pre-edit) content. When {@code withOutOfScopeDeletedFlow} is {@code true}, a second flow that was also
     * deleted from Kestra but does NOT match {@code flowsFilter} is created up front, and the test asserts it
     * remains on the branch after the push, proving the filter genuinely scopes which deletions are staged.
     */
    private void runDeleteOnlyScenario(String flowsFilter, String commitMessage, boolean withOutOfScopeDeletedFlow) throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        FlowWithSource keptFlow = this.createFlow(tenantId, "kept-flow", sourceNamespace);
        FlowWithSource deletedFlow = this.createFlow(tenantId, "deleted-flow", sourceNamespace);
        FlowWithSource outOfScopeDeletedFlow = withOutOfScopeDeletedFlow ? this.createFlow(tenantId, "other-flow", sourceNamespace) : null;

        PushFlows syncPush = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofValue("Baseline - both flows"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .sourceNamespace(Property.ofExpression("{{sourceNamespace}}"))
            .targetNamespace(Property.ofExpression("{{targetNamespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        // The DELETE_ONLY push clones into this context's working dir; it is what the finally block uses for cleanup.
        RunContext deleteOnlyContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);

        try {
            // 1. Baseline SYNC push of all flows.
            syncPush.run(runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory));

            String keptOriginalContent = namespaceRewritten(keptFlow.getSource(), sourceNamespace, targetNamespace);

            // 2. Modify the kept flow directly in Kestra (a pending change we do NOT want to push).
            FlowWithSource keptReloaded = flowRepositoryInterface.findByNamespaceWithSource(tenantId, sourceNamespace).stream()
                .filter(f -> f.getId().equals(keptFlow.getId()))
                .findFirst()
                .orElseThrow();
            String editedSource = keptReloaded.getSource().replace("Hello from my-task", "CHANGED after baseline");
            flowRepositoryInterface.update(GenericFlow.fromYaml(tenantId, editedSource).toBuilder().source(editedSource).build(), keptReloaded);

            // 3. Delete the target flow(s) from Kestra.
            flowRepositoryInterface.delete(deletedFlow);
            if (outOfScopeDeletedFlow != null) {
                flowRepositoryInterface.delete(outOfScopeDeletedFlow);
            }

            // 4. DELETE_ONLY push: stage only the removal of flows matching flowsFilter, leaving everything else untouched.
            PushFlows.PushFlowsBuilder<?, ?> deleteOnlyBuilder = syncPush.toBuilder()
                .commitMessage(Property.ofValue(commitMessage))
                .delete(Property.ofValue(true))
                .pushMode(Property.ofValue(AbstractPushTask.PushMode.DELETE_ONLY));
            if (flowsFilter != null) {
                deleteOnlyBuilder.flows(flowsFilter);
            }
            PushFlows.Output deleteOnlyOutput = deleteOnlyBuilder.build().run(deleteOnlyContext);

            // 5. Verify the branch: deletedFlow gone, keptFlow still holds its ORIGINAL (unmodified) content.
            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();
            Clone.Output cloneOutput = clone.run(runContextFactory.of());

            File deletedFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, deletedFlow.getId() + ".yml").toString());
            assertThat("deleted flow should be removed from Git", deletedFile.exists(), is(false));

            File keptFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, keptFlow.getId() + ".yml").toString());
            assertThat("kept flow should remain on the branch", keptFile.exists(), is(true));
            String keptContentOnBranch = FileUtils.readFileToString(keptFile, "UTF-8");
            assertThat("kept flow must keep its original content; the unrelated change must not ride along", keptContentOnBranch, is(keptOriginalContent));
            assertThat(keptContentOnBranch, not(containsString("CHANGED after baseline")));

            if (outOfScopeDeletedFlow != null) {
                File outOfScopeFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory, outOfScopeDeletedFlow.getId() + ".yml").toString());
                assertThat("flow deleted from Kestra but not matching the flows filter must be preserved on the branch", outOfScopeFile.exists(), is(true));
            }

            // The deletion diff removes exactly as many lines as the pushed flow's source has, whatever that template happens to be.
            long deletedFlowLineCount = deletedFlow.getSource().lines().count();
            assertDiffs(
                deleteOnlyContext,
                deleteOnlyOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+0", "deletions", "-" + deletedFlowLineCount, "changes", "0", "file", gitDirectory + "/" + deletedFlow.getId() + ".yml")
                )
            );
        } finally {
            try {
                this.deleteRemoteBranch(deleteOnlyContext.workingDir().path(), branch);
            } catch (Exception ignored) {
            }
        }
    }

    private RunContext runContext(String tenantId, String repositoryUrl, String gitUserEmail, String gitUserName, String branch, String sourceNamespace, String targetNamespace,
        String gitDirectory) {
        return runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory, null, false);
    }

    private RunContext runContext(String tenantId, String url, String authorEmail, String authorName, String branch, String sourceNamespace, String targetNamespace, String gitDirectory,
        List<String> flows, boolean useStringPebbleArray) {
        Map<String, Object> map = new HashMap<>(
            Map.of(
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
                "sourceNamespace", sourceNamespace,
                "targetNamespace", targetNamespace,
                "gitDirectory", gitDirectory
            )
        );

        if (flows != null && !flows.isEmpty()) {
            if (useStringPebbleArray) {
                map.put("flows", flows);
            } else {
                for (int i = 0; i < flows.size(); i++) {
                    map.put("flow" + (i + 1), flows.get(i));
                }
            }
        }
        var rc = runContextFactory.of(map);
        runContextFactory.initializer().forExecutor((DefaultRunContext) rc);
        return rc;
    }

    private static RevCommit assertIsLastCommit(RunContext cloneRunContext, PushFlows.Output pushOutput) throws IOException, GitAPIException {
        RevCommit revCommit;
        try (Git git = Git.open(cloneRunContext.workingDir().path().toFile())) {
            revCommit = StreamSupport.stream(git.log().setMaxCount(1).call().spliterator(), false).findFirst().orElse(null);
        }
        assertThat(revCommit.getId().getName(), is(pushOutput.getCommitId()));

        return revCommit;
    }

    private static void assertAuthor(RevCommit revCommit, String authorEmail, String authorName) {
        assertThat(revCommit.getAuthorIdent().getEmailAddress(), is(authorEmail));
        assertThat(revCommit.getAuthorIdent().getName(), is(authorName));
        assertThat(revCommit.getCommitterIdent().getEmailAddress(), is(authorEmail));
        assertThat(revCommit.getCommitterIdent().getName(), is(authorName));
    }

    /**
     * Mirrors PushFlows' anchored {@code ^(\s*namespace:\s*)<sourceNamespace>} rewrite, so the oracle can't
     * pass/fail spuriously if {@code sourceNamespace} ever appears outside the {@code namespace:} line.
     */
    private static String namespaceRewritten(String source, String sourceNamespace, String targetNamespace) {
        return source.replaceAll("(?m)^(\\s*namespace:\\s*)" + sourceNamespace, "$1" + targetNamespace);
    }

    private static void assertDiffs(RunContext runContext, URI diffFileUri, List<Map<String, String>> expectedDiffs) throws IOException {
        String diffSummary = IOUtils.toString(runContext.storage().getFile(diffFileUri), StandardCharsets.UTF_8);
        List<Map<String, String>> diffMaps = diffSummary.lines()
            .map(
                Rethrow.throwFunction(
                    diff -> JacksonMapper.ofIon().readValue(
                        diff,
                        new TypeReference<Map<String, String>>() {
                        }
                    )
                )
            )
            .toList();
        assertThat(diffMaps, containsInAnyOrder(expectedDiffs.toArray(Map[]::new)));
    }

    private void deleteRemoteBranch(Path gitDirectory, String branchName) throws GitAPIException, IOException {
        try (Git git = Git.open(gitDirectory.toFile())) {
            git.checkout().setName("tmp").setCreateBranch(true).call();
            git.branchDelete().setBranchNames(R_HEADS + branchName).call();
            RefSpec refSpec = new RefSpec()
                .setSource(null)
                .setDestination(R_HEADS + branchName);
            git.push().setCredentialsProvider(new UsernamePasswordCredentialsProvider(pat, pat)).setRefSpecs(refSpec).setRemote("origin").call();
        }
    }

    private FlowWithSource createFlow(String tenantId, String namespace) {
        return this.createFlow(tenantId, "some-flow", namespace);
    }

    private FlowWithSource createFlow(String tenantId, String flowId, String namespace) {
        String flowSource = """
            id:\s""" + flowId + """

            namespace:\s""" + namespace + """

            tasks:
              - id: my-task
                type: io.kestra.plugin.core.log.Log
                message: Hello from my-task
              - id: subflow
                type: io.kestra.plugin.core.flow.Subflow
                namespace:\s""" + namespace + """
            .sub-namespace
                flowId: another-flow
            """;

        return flowRepositoryInterface.create(GenericFlow.fromYaml(tenantId, flowSource));
    }
}
