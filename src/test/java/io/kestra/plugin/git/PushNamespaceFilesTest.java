package io.kestra.plugin.git;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.Rethrow;
import io.kestra.plugin.git.services.GitService;

import jakarta.inject.Inject;

import static org.eclipse.jgit.lib.Constants.R_HEADS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class PushNamespaceFilesTest extends AbstractGitTest {
    public static final String DESCRIPTION = "One-task push";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void defaultCase_SingleRegex() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, namespace, gitDirectory);

        String firstFilePath = "first-file.txt";
        runContext.storage().namespace(namespace).putFile(Path.of(firstFilePath), new ByteArrayInputStream("First file".getBytes()));
        String secondFilePath = "nested/second-file.txt";
        String secondFileContent = "Second file";
        runContext.storage().namespace(namespace).putFile(Path.of(secondFilePath), new ByteArrayInputStream(secondFileContent.getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .files("nested/*")
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);
            GitService gitService = new GitService(pushNamespaceFiles);
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

            File nsFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), firstFilePath);
            assertThat(nsFile.exists(), is(false));

            nsFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), secondFilePath);
            assertThat(nsFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(nsFile, "UTF-8");
            assertThat(fileContent, is(secondFileContent));

            assertThat(pushOutput.getCommitURL(), is(repositoryUrl + "/commit/" + pushOutput.getCommitId()));

            assertDiffs(
                runContext,
                pushOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + secondFilePath)
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
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, namespace, gitDirectory);

        String firstFilePath = "first-file.txt";
        runContext.storage().namespace(namespace).putFile(Path.of(firstFilePath), new ByteArrayInputStream("First file".getBytes()));
        String secondFilePath = "nested/second-file.txt";
        String secondFileContent = "Second file";
        runContext.storage().namespace(namespace).putFile(Path.of(secondFilePath), new ByteArrayInputStream(secondFileContent.getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .files("second*")
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .dryRun(Property.ofValue(true))
            .build();

        PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);
        GitService gitService = new GitService(pushNamespaceFiles);
        assertThat(gitService.branchExists(runContext, branch), is(false));

        assertThat(pushOutput.getCommitURL(), nullValue());

        assertDiffs(
            runContext,
            pushOutput.diffFileUri(),
            List.of(
                Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + secondFilePath)
            )
        );
    }

    @Test
    void defaultCase_SingleRegex_DeleteScopedToRegex() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, namespace, gitDirectory);

        String nonMatchingFilePath = "first-file.txt";
        String nonMatchingFileContent = "First file";
        runContext.storage().namespace(namespace).putFile(Path.of(nonMatchingFilePath), new ByteArrayInputStream(nonMatchingFileContent.getBytes()));
        String matchingFilePath = "nested/second-file.txt";
        String matchingFileContent = "Second file";
        runContext.storage().namespace(namespace).putFile(Path.of(matchingFilePath), new ByteArrayInputStream(matchingFileContent.getBytes()));
        String fileDeletedAfterSecondPushPath = "second-deleted-after-second-push.txt";
        String fileDeletedAfterSecondPushContent = "File deleted after second push";
        runContext.storage().namespace(namespace).putFile(Path.of(fileDeletedAfterSecondPushPath), new ByteArrayInputStream(fileDeletedAfterSecondPushContent.getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushFlows.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            Clone.Output cloneOutput = clone.run(runContextFactory.of());

            File flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), nonMatchingFilePath);
            assertThat(flowFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(nonMatchingFileContent));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), matchingFilePath);
            assertThat(flowFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(matchingFileContent));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), fileDeletedAfterSecondPushPath);
            assertThat(flowFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(fileDeletedAfterSecondPushContent));

            assertDiffs(
                runContext,
                pushOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + nonMatchingFilePath),
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + matchingFilePath),
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + fileDeletedAfterSecondPushPath)
                )
            );

            runContext.storage().namespace(namespace).delete(Path.of(fileDeletedAfterSecondPushPath));
            pushOutput = pushNamespaceFiles.toBuilder()
                .files("second*")
                .build().run(runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, namespace, gitDirectory));

            cloneOutput = clone.run(runContextFactory.of());

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), nonMatchingFilePath);
            assertThat(flowFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(nonMatchingFileContent));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), matchingFilePath);
            assertThat(flowFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(flowFile, "UTF-8");
            assertThat(fileContent, is(matchingFileContent));

            flowFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), fileDeletedAfterSecondPushPath);
            assertThat(flowFile.exists(), is(false));

            assertDiffs(
                runContext,
                pushOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+0", "deletions", "-1", "changes", "0", "file", gitDirectory + "/" + fileDeletedAfterSecondPushPath)
                )
            );
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void defaultCase_NoRegex() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, namespace, gitDirectory);

        String firstFilePath = "first-file.txt";
        String firstFileContent = "First file";
        runContext.storage().namespace(namespace).putFile(Path.of(firstFilePath), new ByteArrayInputStream(firstFileContent.getBytes()));
        String secondFilePath = "nested/second-file.txt";
        String secondFileContent = "Second file";
        runContext.storage().namespace(namespace).putFile(Path.of(secondFilePath), new ByteArrayInputStream(secondFileContent.getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

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

            File nsFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), firstFilePath);
            assertThat(nsFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(nsFile, "UTF-8");
            assertThat(fileContent, is(firstFileContent));

            nsFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), secondFilePath);
            assertThat(nsFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(nsFile, "UTF-8");
            assertThat(fileContent, is(secondFileContent));

            assertThat(pushOutput.getCommitURL(), is(repositoryUrl + "/commit/" + pushOutput.getCommitId()));

            assertDiffs(
                runContext,
                pushOutput.getFiles(),
                List.of(
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + firstFilePath),
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + secondFilePath)
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
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, namespace, gitDirectory);

        String firstFilePath = "first-file.txt";
        String firstFileContent = "First file";
        runContext.storage().namespace(namespace).putFile(Path.of(firstFilePath), new ByteArrayInputStream(firstFileContent.getBytes()));
        String secondFilePath = "nested/second-file.txt";
        String secondFileContent = "Second file";
        runContext.storage().namespace(namespace).putFile(Path.of(secondFilePath), new ByteArrayInputStream(secondFileContent.getBytes()));
        String thirdFilePath = "third-file.txt";
        runContext.storage().namespace(namespace).putFile(Path.of(thirdFilePath), new ByteArrayInputStream("Third file".getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .files(List.of("first*", "second*"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

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

            File nsFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), firstFilePath);
            assertThat(nsFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(nsFile, "UTF-8");
            assertThat(fileContent, is(firstFileContent));

            nsFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), secondFilePath);
            assertThat(nsFile.exists(), is(true));
            fileContent = FileUtils.readFileToString(nsFile, "UTF-8");
            assertThat(fileContent, is(secondFileContent));

            nsFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), thirdFilePath);
            assertThat(nsFile.exists(), is(false));

            assertThat(pushOutput.getCommitURL(), is(repositoryUrl + "/commit/" + pushOutput.getCommitId()));

            assertDiffs(
                runContext,
                pushOutput.getFiles(),
                List.of(
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + firstFilePath),
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + secondFilePath)
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
    void defaultCase_NoRegexNoAuthor() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, "", "", branch, namespace, gitDirectory);

        runContext.storage().namespace(namespace).putFile(Path.of(StorageContext.namespaceFilePrefix(namespace) + "/first-file.txt"), new ByteArrayInputStream("First file".getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

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
    void shouldFailIfNoFilesMatched() {
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, namespace, gitDirectory);

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles-fail-on-missing")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .files("nonexistent-file.txt")
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .errorOnMissing(Property.ofValue(true))
            .build();

        assertThrows(KestraRuntimeException.class, () ->
        {
            pushNamespaceFiles.run(runContext);
        });
    }

    @Test
    void kestraIgnore_IgnoresPushAndSkipsDeletion() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, namespace, gitDirectory);

        // Clone seed OUTSIDE the workingDir so that workingDir remains empty for the task's clone
        Path seedClone = Files.createTempDirectory("seed-clone-");

        try (
            Git git = Git.cloneRepository()
                .setURI(repositoryUrl)
                .setDirectory(seedClone.toFile())
                .setCredentialsProvider(new UsernamePasswordCredentialsProvider(pat, pat))
                .call()
        ) {

            git.checkout().setCreateBranch(true).setName(branch).call();

            Path dir = seedClone.resolve(gitDirectory);
            Files.createDirectories(dir);
            Files.writeString(
                dir.resolve(".kestraignore"),
                String.join(
                    "\n",
                    "ignored-file.txt",
                    "ignored-dir/"
                ),
                StandardCharsets.UTF_8
            );

            Path ignoredDir = dir.resolve("ignored-dir");
            Files.createDirectories(ignoredDir);
            Files.writeString(ignoredDir.resolve("existing.txt"), "pre-existing ignored content", StandardCharsets.UTF_8);

            git.add().addFilepattern(".").call();
            git.commit().setMessage("seed: add .kestraignore and pre-existing ignored file").call();
            git.push().setCredentialsProvider(new UsernamePasswordCredentialsProvider(pat, pat)).call();
        }

        // Put files in namespace storage: one ignored and one included
        String ignoredFilePath = "ignored-file.txt";
        String includedFilePath = "included.txt";
        runContext.storage().namespace(namespace).putFile(Path.of(ignoredFilePath), new ByteArrayInputStream("IGNORED".getBytes(StandardCharsets.UTF_8)));
        runContext.storage().namespace(namespace).putFile(Path.of(includedFilePath), new ByteArrayInputStream("INCLUDED".getBytes(StandardCharsets.UTF_8)));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles-kestraignore")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

            // Fresh clone to verify state after push (this uses workingDir but it's fine post-run)
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
            Path repoDir = Path.of(cloneOutput.getDirectory());

            File included = new File(repoDir.resolve(gitDirectory).toFile(), includedFilePath);
            assertThat(included.exists(), is(true));
            assertThat(FileUtils.readFileToString(included, StandardCharsets.UTF_8), is("INCLUDED"));

            File ignored = new File(repoDir.resolve(gitDirectory).toFile(), ignoredFilePath);
            assertThat(ignored.exists(), is(false));

            File preserved = new File(repoDir.resolve(gitDirectory).toFile(), "ignored-dir/existing.txt");
            assertThat(preserved.exists(), is(true));
            assertThat(FileUtils.readFileToString(preserved, StandardCharsets.UTF_8), is("pre-existing ignored content"));

            assertDiffs(
                runContext,
                pushOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + includedFilePath)
                )
            );

            RevCommit revCommit = assertIsLastCommit(cloneRunContext, pushOutput);
            assertThat(revCommit.getFullMessage(), is("Push from CI - " + DESCRIPTION));
            assertAuthor(revCommit, gitUserEmail, gitUserName);
        } finally {
            // Clean up remote branch using the seed clone path
            try {
                this.deleteRemoteBranch(seedClone, branch);
            } finally {
                // Best effort local cleanup
                FileUtils.deleteQuietly(seedClone.toFile());
            }
        }
    }

    @Test
    void namespaceDirectory_ScopesAndStripsPushedPaths() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, namespace, gitDirectory);

        String outsidePrefixPath = "outside.txt";
        runContext.storage().namespace(namespace).putFile(Path.of(outsidePrefixPath), new ByteArrayInputStream("outside prefix".getBytes()));
        String includedPath = "shared-scripts/included.txt";
        String includedContent = "included content";
        runContext.storage().namespace(namespace).putFile(Path.of(includedPath), new ByteArrayInputStream(includedContent.getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles-namespace-directory")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespaceDirectory(Property.ofValue("/shared-scripts"))
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            Clone.Output cloneOutput = clone.run(runContextFactory.of());
            Path repoGitDirectory = Path.of(cloneOutput.getDirectory(), gitDirectory);

            // prefix stripped: lands at <gitDirectory>/included.txt, not <gitDirectory>/shared-scripts/included.txt
            File includedFile = new File(repoGitDirectory.toString(), "included.txt");
            assertThat(includedFile.exists(), is(true));
            assertThat(FileUtils.readFileToString(includedFile, "UTF-8"), is(includedContent));

            // outside the prefix: never pushed
            assertThat(new File(repoGitDirectory.toString(), outsidePrefixPath).exists(), is(false));
            assertThat(new File(repoGitDirectory.toString(), "shared-scripts").exists(), is(false));

            assertDiffs(
                runContext,
                pushOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/included.txt")
                )
            );
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void namespaceDirectory_FilesGlobMatchesFullNamespacePath() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, namespace, gitDirectory);

        String matchingPath = "shared-scripts/foo.py";
        String matchingContent = "print(1)";
        runContext.storage().namespace(namespace).putFile(Path.of(matchingPath), new ByteArrayInputStream(matchingContent.getBytes()));
        String nonMatchingPath = "shared-scripts/bar.txt";
        runContext.storage().namespace(namespace).putFile(Path.of(nonMatchingPath), new ByteArrayInputStream("not a python file".getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles-namespace-directory-glob")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespaceDirectory(Property.ofValue("/shared-scripts"))
            // proves `files` still matches against the full namespace-relative path, independently of namespaceDirectory
            .files("shared-scripts/*.py")
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .build();

            Clone.Output cloneOutput = clone.run(runContextFactory.of());
            Path repoGitDirectory = Path.of(cloneOutput.getDirectory(), gitDirectory);

            assertThat(new File(repoGitDirectory.toString(), "foo.py").exists(), is(true));
            assertThat(new File(repoGitDirectory.toString(), "bar.txt").exists(), is(false));

            assertDiffs(
                runContext,
                pushOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/foo.py")
                )
            );
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void namespaceDirectory_SyncPushRoundTrip_ProducesEmptyDiff() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String sourceNamespace = IdUtils.create().toLowerCase();
        String roundTripNamespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        RunContext runContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, sourceNamespace, gitDirectory);
        String filePath = "shared-scripts/foo.py";
        String fileContent = "print(1)";
        runContext.storage().namespace(sourceNamespace).putFile(Path.of(filePath), new ByteArrayInputStream(fileContent.getBytes()));

        PushNamespaceFiles firstPush = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles-round-trip-seed")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespaceDirectory(Property.ofValue("/shared-scripts"))
            .build();

        try {
            firstPush.run(runContext);

            SyncNamespaceFiles sync = SyncNamespaceFiles.builder()
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(branch))
                .gitDirectory(Property.ofValue(gitDirectory))
                .namespace(Property.ofValue(roundTripNamespace))
                .namespaceDirectory(Property.ofValue("/shared-scripts"))
                .build();
            sync.run(runContextFactory.of());

            assertNamespaceFileContent(runContextFactory.of(), roundTripNamespace, filePath, fileContent);

            RunContext secondPushRunContext = runContext(tenantId, repositoryUrl, gitUserEmail, gitUserName, branch, roundTripNamespace, gitDirectory);
            PushNamespaceFiles secondPush = firstPush.toBuilder()
                .id("pushNamespaceFiles-round-trip-verify")
                .namespace(Property.ofExpression("{{namespace}}"))
                .build();
            PushNamespaceFiles.Output secondPushOutput = secondPush.run(secondPushRunContext);

            // identical content already on the branch: nothing to commit, proving the round trip is a no-op
            assertThat(secondPushOutput.getCommitId(), nullValue());
        } finally {
            this.deleteRemoteBranch(runContext.workingDir().path(), branch);
        }
    }

    @Test
    void namespaceDirectory_Delete_PreservesFilesOutsidePrefix() throws Exception {
        String tenantId = TenantService.MAIN_TENANT;
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";

        Path remoteRepo = createLocalBareRepo();
        seedBranch(remoteRepo, branch, Map.of(gitDirectory + "/existing/outside.txt", "outside prefix content"));

        RunContext runContext = runContext(tenantId, localUrl(remoteRepo), gitUserEmail, gitUserName, branch, namespace, gitDirectory);
        runContext.storage().namespace(namespace).putFile(Path.of("shared-scripts/keep.txt"), new ByteArrayInputStream("kept".getBytes(StandardCharsets.UTF_8)));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles-namespace-directory-preserve")
            .type(PushNamespaceFiles.class.getName())
            .branch(Property.ofExpression("{{branch}}"))
            .url(Property.ofExpression("{{url}}"))
            .commitMessage(Property.ofExpression("Push from CI - {{description}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .authorEmail(Property.ofExpression("{{email}}"))
            .authorName(Property.ofExpression("{{name}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespaceDirectory(Property.ofValue("/shared-scripts"))
            .build();

        // delete defaults to true: without the namespaceDirectory-aware preserve fix, this scoped push would wipe
        // out `existing/outside.txt`, which is outside the prefix but already checked out in gitDirectory
        pushNamespaceFiles.run(runContext);

        Clone clone = Clone.builder()
            .id("clone")
            .type(Clone.class.getName())
            .url(Property.ofValue(localUrl(remoteRepo)))
            .branch(Property.ofValue(branch))
            .build();
        Clone.Output cloneOutput = clone.run(runContextFactory.of());
        Path repoGitDirectory = Path.of(cloneOutput.getDirectory(), gitDirectory);

        File preserved = new File(repoGitDirectory.toString(), "existing/outside.txt");
        assertThat(preserved.exists(), is(true));
        assertThat(FileUtils.readFileToString(preserved, StandardCharsets.UTF_8), is("outside prefix content"));

        File keepFile = new File(repoGitDirectory.toString(), "keep.txt");
        assertThat(keepFile.exists(), is(true));
        assertThat(FileUtils.readFileToString(keepFile, StandardCharsets.UTF_8), is("kept"));
    }

    // Creates a local, non-bare-backed git repository usable as a push remote without external credentials or
    // network access: a bare repo (so pushes are never rejected as updating "the currently checked out branch")
    // seeded with one commit on its default branch.
    private Path createLocalBareRepo() throws Exception {
        Path repoDir = Files.createTempDirectory("unit-test.push-namespace-directory-bare");
        Git.init().setDirectory(repoDir.toFile()).setBare(true).call().close();

        Path seedClone = Files.createTempDirectory("unit-test.push-namespace-directory-seed");
        try (Git git = Git.cloneRepository().setURI(repoDir.toUri().toString()).setDirectory(seedClone.toFile()).call()) {
            Files.writeString(seedClone.resolve("README.md"), "seed");
            git.add().addFilepattern(".").call();
            git.commit().setMessage("seed commit").setAuthor("test", "test@test.com").call();
            git.push().call();
        } finally {
            FileUtils.deleteQuietly(seedClone.toFile());
        }
        return repoDir;
    }

    // Pushes the given files as a new commit on a new branch of the local bare repo, so the branch already exists
    // with pre-existing content before the task under test clones and pushes to it.
    private void seedBranch(Path bareRepoDir, String branch, Map<String, String> filesByRelativePath) throws Exception {
        Path seedClone = Files.createTempDirectory("unit-test.push-namespace-directory-branch-seed");
        try (Git git = Git.cloneRepository().setURI(bareRepoDir.toUri().toString()).setDirectory(seedClone.toFile()).call()) {
            git.checkout().setCreateBranch(true).setName(branch).call();
            for (Map.Entry<String, String> entry : filesByRelativePath.entrySet()) {
                Path filePath = seedClone.resolve(entry.getKey());
                Files.createDirectories(filePath.getParent());
                Files.writeString(filePath, entry.getValue());
            }
            git.add().addFilepattern(".").call();
            git.commit().setMessage("branch seed commit").setAuthor("test", "test@test.com").call();
            git.push().call();
        } finally {
            FileUtils.deleteQuietly(seedClone.toFile());
        }
    }

    private static String localUrl(Path repoDir) {
        return repoDir.toUri().toString();
    }

    private static void assertNamespaceFileContent(RunContext runContext, String namespace, String namespaceFileUri, String expectedFileContent) throws IOException {
        try (InputStream is = runContext.storage().namespace(namespace).getFileContent(Path.of(namespaceFileUri))) {
            assertThat(IOUtils.toString(is, StandardCharsets.UTF_8), is(expectedFileContent));
        }
    }

    private RunContext runContext(String tenantId, String url, String authorEmail, String authorName, String branch, String namespace, String gitDirectory) {
        return runContextFactory.of(
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
                "namespace", namespace,
                "gitDirectory", gitDirectory
            )
        );
    }

    private static RevCommit assertIsLastCommit(RunContext cloneRunContext, PushNamespaceFiles.Output pushOutput) throws IOException, GitAPIException {
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
}
