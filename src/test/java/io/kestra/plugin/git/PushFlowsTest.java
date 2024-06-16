package io.kestra.plugin.git;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.serializers.YamlFlowParser;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.Rethrow;
import io.kestra.plugin.git.services.GitService;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.eclipse.jgit.lib.Constants.R_HEADS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class PushFlowsTest extends AbstractGitTest {
    public static final String DESCRIPTION = "One-task push";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private YamlFlowParser yamlFlowParser;

    @Inject
    private FlowRepositoryInterface flowRepositoryInterface;

    @Test
    void hardcodedPassword() {
        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .url(repositoryUrl)
            .password("my-password")
            .build();

        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> pushFlows.run(runContextFactory.of(Map.of(
            "flow", Map.of(
                "tenantId", "tenantId",
                "namespace", "system"
            ))))
        );
        assertThat(illegalArgumentException.getMessage(), is("It looks like you have hard-coded Git credentials. Make sure to pass the credential securely using a Pebble expression (e.g. using secrets or environment variables)."));
    }

    @Test
    void defaultCase_SingleRegex() throws Exception {
        String tenantId = "my-tenant";
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
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .authorName("{{name}}")
            .sourceNamespace("{{sourceNamespace}}")
            .targetNamespace("{{targetNamespace}}")
            .flows("second*")
            .includeChildNamespaces(true)
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);
            GitService gitService = new GitService(pushFlows);
            assertThat(gitService.branchExists(runContext, branch), is(true));

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(repositoryUrl)
                .username(pat)
                .password(pat)
                .branch(branch)
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
    void defaultCase_SingleRegexDryRun() throws Exception {
        String tenantId = "my-tenant";
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
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .authorName("{{name}}")
            .sourceNamespace("{{sourceNamespace}}")
            .targetNamespace("{{targetNamespace}}")
            .flows("second*")
            .includeChildNamespaces(true)
            .gitDirectory("{{gitDirectory}}")
            .dryRun(true)
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
        String tenantId = "my-tenant";
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
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .authorName("{{name}}")
            .sourceNamespace("{{sourceNamespace}}")
            .targetNamespace("{{targetNamespace}}")
            .includeChildNamespaces(true)
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(repositoryUrl)
                .username(pat)
                .password(pat)
                .branch(branch)
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
        String tenantId = "my-tenant";
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
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .authorName("{{name}}")
            .sourceNamespace("{{sourceNamespace}}")
            .targetNamespace("{{targetNamespace}}")
            .includeChildNamespaces(true)
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(repositoryUrl)
                .username(pat)
                .password(pat)
                .branch(branch)
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

    @Test
    void defaultCase_MultipleRegex() throws Exception {
        String tenantId = "my-tenant";
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, targetNamespace, gitDirectory);

        FlowWithSource createdFlow = this.createFlow(tenantId, "first-flow", sourceNamespace);
        String subNamespace = "sub-namespace";
        FlowWithSource createdSubNsFlow = this.createFlow(tenantId, "second-flow", sourceNamespace + "." + subNamespace);
        FlowWithSource thirdFlow = this.createFlow(tenantId, "third-flow", sourceNamespace);

        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .authorName("{{name}}")
            .sourceNamespace("{{sourceNamespace}}")
            .targetNamespace("{{targetNamespace}}")
            .flows(List.of("first*", "second*"))
            .includeChildNamespaces(true)
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(repositoryUrl)
                .username(pat)
                .password(pat)
                .branch(branch)
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
        String tenantId = "my-tenant";
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
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .sourceNamespace("{{sourceNamespace}}")
            .targetNamespace("{{targetNamespace}}")
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(repositoryUrl)
                .username(pat)
                .password(pat)
                .branch(branch)
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
        String tenantId = "my-tenant";
        String sourceNamespace = IdUtils.create().toLowerCase();
        String targetNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-flows";

        RunContext runContext = runContext(tenantId, repositoryUrl, "", "", branch, sourceNamespace, targetNamespace, gitDirectory);

        this.createFlow(tenantId, sourceNamespace);

        PushFlows pushFlows = PushFlows.builder()
            .id("pushFlows")
            .type(PushFlows.class.getName())
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .sourceNamespace("{{sourceNamespace}}")
            .targetNamespace("{{targetNamespace}}")
            .includeChildNamespaces(true)
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushFlows.Output pushOutput = pushFlows.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(repositoryUrl)
                .username(pat)
                .password(pat)
                .branch(branch)
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

    private RunContext runContext(String tenantId, String url, String authorEmail, String authorName, String branch, String sourceNamespace, String targetNamespace, String gitDirectory) {
        return runContextFactory.of(Map.of(
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
        ));
    }

    private static RevCommit assertIsLastCommit(RunContext cloneRunContext, PushFlows.Output pushOutput) throws IOException, GitAPIException {
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

    private static void assertDiffs(RunContext runContext, URI diffFileUri, List<Map<String, String>> expectedDiffs) throws IOException {
        String diffSummary = IOUtils.toString(runContext.storage().getFile(diffFileUri), StandardCharsets.UTF_8);
        List<Map<String, String>> diffMaps = diffSummary.lines()
            .map(Rethrow.throwFunction(diff -> JacksonMapper.ofIon().readValue(
                diff,
                new TypeReference<Map<String, String>>() {}
            )))
            .toList();
        assertThat(diffMaps, containsInAnyOrder(expectedDiffs.toArray(Map[]::new)));
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

    private FlowWithSource createFlow(String tenantId, String namespace) {
        return this.createFlow(tenantId, "some-flow", namespace);
    }

    private FlowWithSource createFlow(String tenantId, String flowId, String namespace) {
        String flowSource = """
            id:\s""" + flowId + """
            
            namespace:\s""" + namespace + """
            
            tasks:
              - id: my-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from my-task
              - id: subflow
                type: io.kestra.core.tasks.flows.Subflow
                namespace:\s""" + namespace + """
            .sub-namespace
                flowId: another-flow
            """;
        Flow flow = yamlFlowParser.parse(flowSource, Flow.class).toBuilder()
            .tenantId(tenantId)
            .build();
        return flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        );
    }
}
