package io.kestra.plugin.git;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.git.services.GitService;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.eclipse.jgit.lib.Constants.R_HEADS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class PushExecutionFilesTest extends AbstractGitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void pushSingleFileWithGlob() throws Exception {
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();

        Map<String, Object> outputs = Map.of(
            "generate", Map.of(
                "outputFiles", Map.of(
                    "report.txt", "kestra://"
                        + StorageContext.namespaceFilePrefix(namespace)
                        + "/report.txt"
                )
            )
        );

        RunContext runContext = runContextFactory.of(Map.of(
            "flow", Map.of(
                "tenantId", TenantService.MAIN_TENANT,
                "namespace", namespace
            ),
            "url", repositoryUrl,
            "branch", branch,
            "pat", pat,
            "outputs", outputs
        ));

        String filePath = "report.txt";
        String fileContent = "hello from storage";
        runContext.storage().namespace(namespace)
            .putFile(Path.of(filePath), new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))
        );

        PushExecutionFiles push = PushExecutionFiles.builder()
            .id("push")
            .type(PushExecutionFiles.class.getName())
            .url(Property.ofExpression("{{ url }}"))
            .username(Property.ofExpression("{{ pat }}"))
            .password(Property.ofExpression("{{ pat }}"))
            .branch(Property.ofExpression("{{ branch }}"))
            .files("*.txt")
            .gitDirectory(Property.ofValue("reports"))
            .commitMessage(Property.ofValue("push test"))
            .build();

        try {
            PushExecutionFiles.Output out = push.run(runContext);
            assertThat(out.getCommitId(), notNullValue());

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(new Property<>(repositoryUrl))
                .username(new Property<>(pat))
                .password(new Property<>(pat))
                .branch(new Property<>(branch))
                .build();

            Clone.Output cloneOutput = clone.run(runContextFactory.of());
            File pushedFile = new File(Path.of(cloneOutput.getDirectory(), "reports").toFile(), filePath);
            assertThat(pushedFile.exists(), is(true));
            assertThat(FileUtils.readFileToString(pushedFile, StandardCharsets.UTF_8), is(fileContent));
        } finally {
            deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void pushFilesMapRenamed() throws Exception {
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();

        RunContext runContext = runContext(namespace, branch);

        String filePath = "report.txt";
        String fileContent = "log here";
        var nsFile = runContext.storage().namespace(namespace)
            .putFile(Path.of(filePath),
            new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))
        ).stream().filter(namespaceFile -> !namespaceFile.isDirectory()).findFirst().orElseThrow();

        PushExecutionFiles push = PushExecutionFiles.builder()
            .id("push")
            .type(PushExecutionFiles.class.getName())
            .url(Property.ofExpression("{{ url }}"))
            .username(Property.ofExpression("{{ pat }}"))
            .password(Property.ofExpression("{{ pat }}"))
            .branch(Property.ofExpression("{{ branch }}"))
            .filesMap(Map.of("renamed.log", nsFile.uri().toString()))
            .gitDirectory(Property.ofValue("logs"))
            .commitMessage(Property.ofValue("push log"))
            .build();

        try {
            PushExecutionFiles.Output out = push.run(runContext);
            assertThat(out.getCommitId(), notNullValue());

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(new Property<>(repositoryUrl))
                .username(new Property<>(pat))
                .password(new Property<>(pat))
                .branch(new Property<>(branch))
                .build();

            Clone.Output cloneOutput = clone.run(runContextFactory.of());
            File pushedFile = new File(Path.of(cloneOutput.getDirectory(), "logs").toFile(), "renamed.log");
            assertThat(pushedFile.exists(), is(true));
            assertThat(FileUtils.readFileToString(pushedFile, StandardCharsets.UTF_8), is(fileContent));
        } finally {
            deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void skipIfNoMatch() throws Exception {
        String branch = IdUtils.create();
        String namespace = IdUtils.create().toLowerCase();

        RunContext runContext = runContext(namespace, branch);

        PushExecutionFiles push = PushExecutionFiles.builder()
            .id("push")
            .type(PushExecutionFiles.class.getName())
            .url(Property.ofExpression("{{ url }}"))
            .username(Property.ofExpression("{{ pat }}"))
            .password(Property.ofExpression("{{ pat }}"))
            .branch(Property.ofExpression("{{ branch }}"))
            .files("*.json")
            .gitDirectory(Property.ofValue("reports"))
            .commitMessage(Property.ofValue("skip test"))
            .errorOnMissing(Property.ofValue(false))
            .build();

        try {
            PushExecutionFiles.Output out = push.run(runContext);
            assertThat(out.getCommitId(), nullValue());
        } finally {
            deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void dryRunDoesNotPush() throws Exception {
        String branch = IdUtils.create();
        String namespace = IdUtils.create().toLowerCase();

        RunContext runContext = runContext(namespace, branch);

        String filePath = "file.csv";
        String fileContent = "id,val\n1,2";
        runContext.storage().namespace(namespace)
            .putFile(Path.of(filePath),
            new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))
        );

        PushExecutionFiles push = PushExecutionFiles.builder()
            .id("push")
            .type(PushExecutionFiles.class.getName())
            .url(Property.ofExpression("{{ url }}"))
            .username(Property.ofExpression("{{ pat }}"))
            .password(Property.ofExpression("{{ pat }}"))
            .branch(Property.ofExpression("{{ branch }}"))
            .files("*.csv")
            .gitDirectory(Property.ofValue("reports"))
            .commitMessage(Property.ofValue("dry run"))
            .dryRun(Property.ofValue(true))
            .build();

        try {
            PushExecutionFiles.Output out = push.run(runContext);
            GitService svc = new GitService(push);
            assertThat(svc.branchExists(runContext, branch), is(false));
            assertThat(out.getCommitId(), nullValue());
        } finally {
            deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    private void deleteRemoteBranch(Path gitDirectory, String branchName) throws GitAPIException, IOException {
        Path dotGit = gitDirectory.resolve(".git");
        if (!Files.exists(dotGit)) {
            return;
        }
        try (Git git = Git.open(gitDirectory.toFile())) {
            git.checkout().setName("tmp").setCreateBranch(true).call();
            git.branchDelete().setBranchNames(R_HEADS + branchName).call();
            RefSpec refSpec = new RefSpec()
                .setSource(null)
                .setDestination(R_HEADS + branchName);
            git.push()
                .setCredentialsProvider(new UsernamePasswordCredentialsProvider(pat, pat))
                .setRefSpecs(refSpec)
                .setRemote("origin")
                .call();
        }
    }

    private RunContext runContext(String namespace, String branch) {
        return runContextFactory.of(Map.of(
            "flow", Map.of(
                "tenantId", TenantService.MAIN_TENANT,
                "namespace", namespace
            ),
            "url", repositoryUrl,
            "branch", branch,
            "pat", pat
        ));
    }

}
