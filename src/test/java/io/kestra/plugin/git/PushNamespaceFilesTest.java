package io.kestra.plugin.git;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.Rethrow;
import io.kestra.plugin.git.services.GitService;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
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

@MicronautTest
public class PushNamespaceFilesTest {
    public static final String DESCRIPTION = "One-task push";

    @Value("${kestra.git.pat}")
    private String pat;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storage;

    @Test
    void hardcodedPassword() {
        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .url("https://github.com/kestra-io/unit-tests")
            .password("my-password")
            .build();

        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> pushNamespaceFiles.run(runContextFactory.of(Map.of(
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
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";
        String authorEmail = "bmulier@kestra.io";
        String authorName = "brianmulier";
        String url = "https://github.com/kestra-io/unit-tests";
        RunContext runContext = runContext(tenantId, url, authorEmail, authorName, branch, namespace, gitDirectory);

        String firstFilePath = "first-file.txt";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + firstFilePath), new ByteArrayInputStream("First file".getBytes()));
        String secondFilePath = "nested/second-file.txt";
        String secondFileContent = "Second file";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + secondFilePath), new ByteArrayInputStream(secondFileContent.getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .authorName("{{name}}")
            .namespace("{{namespace}}")
            .files("second*")
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);
            GitService gitService = new GitService(pushNamespaceFiles);
            assertThat(gitService.branchExists(runContext, branch), is(true));

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(url)
                .username(pat)
                .password(pat)
                .branch(branch)
                .build();

            RunContext cloneRunContext = runContextFactory.of();
            Clone.Output cloneOutput = clone.run(cloneRunContext);

            File nsFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), firstFilePath);
            assertThat(nsFile.exists(), is(false));

            nsFile = new File(Path.of(cloneOutput.getDirectory(), gitDirectory).toString(), secondFilePath);
            assertThat(nsFile.exists(), is(true));
            String fileContent = FileUtils.readFileToString(nsFile, "UTF-8");
            assertThat(fileContent, is(secondFileContent));

            assertThat(pushOutput.getCommitURL(), is(url + "/commit/" + pushOutput.getCommitId()));

            assertDiffs(
                runContext,
                pushOutput.diffFileUri(),
                List.of(
                    Map.of("additions", "+1", "deletions", "-0", "changes", "0", "file", gitDirectory + "/" + secondFilePath)
                )
            );

            RevCommit revCommit = assertIsLastCommit(cloneRunContext, pushOutput);
            assertThat(revCommit.getFullMessage(), is("Push from CI - " + DESCRIPTION));
            assertAuthor(revCommit, authorEmail, authorName);
        } finally {
            this.deleteRemoteBranch(runContext.tempDir(), branch);
        }
    }

    @Test
    void defaultCase_SingleRegexDryRun() throws Exception {
        String tenantId = "my-tenant";
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";
        String authorEmail = "bmulier@kestra.io";
        String authorName = "brianmulier";
        String url = "https://github.com/kestra-io/unit-tests";
        RunContext runContext = runContext(tenantId, url, authorEmail, authorName, branch, namespace, gitDirectory);

        String firstFilePath = "first-file.txt";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + firstFilePath), new ByteArrayInputStream("First file".getBytes()));
        String secondFilePath = "nested/second-file.txt";
        String secondFileContent = "Second file";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + secondFilePath), new ByteArrayInputStream(secondFileContent.getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .authorName("{{name}}")
            .namespace("{{namespace}}")
            .files("second*")
            .gitDirectory("{{gitDirectory}}")
            .dryRun(true)
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
        String tenantId = "my-tenant";
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";
        String authorEmail = "bmulier@kestra.io";
        String authorName = "brianmulier";
        String url = "https://github.com/kestra-io/unit-tests";
        RunContext runContext = runContext(tenantId, url, authorEmail, authorName, branch, namespace, gitDirectory);

        String nonMatchingFilePath = "first-file.txt";
        String nonMatchingFileContent = "First file";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + nonMatchingFilePath), new ByteArrayInputStream(nonMatchingFileContent.getBytes()));
        String matchingFilePath = "nested/second-file.txt";
        String matchingFileContent = "Second file";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + matchingFilePath), new ByteArrayInputStream(matchingFileContent.getBytes()));
        String fileDeletedAfterSecondPushPath = "second-deleted-after-second-push.txt";
        String fileDeletedAfterSecondPushContent = "File deleted after second push";
        URI toDeleteURI = URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + fileDeletedAfterSecondPushPath);
        storage.put(tenantId, toDeleteURI, new ByteArrayInputStream(fileDeletedAfterSecondPushContent.getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushFlows.class.getName())
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .authorName("{{name}}")
            .namespace("{{namespace}}")
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(url)
                .username(pat)
                .password(pat)
                .branch(branch)
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

            storage.delete(tenantId, toDeleteURI);
            pushOutput = pushNamespaceFiles.toBuilder()
                .files("second*")
                .build().run(runContext(tenantId, url, authorEmail, authorName, branch, namespace, gitDirectory));

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
            this.deleteRemoteBranch(runContext.tempDir(), branch);
        }
    }

    @Test
    void defaultCase_NoRegex() throws Exception {
        String tenantId = "my-tenant";
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";
        String authorEmail = "bmulier@kestra.io";
        String authorName = "brianmulier";
        String url = "https://github.com/kestra-io/unit-tests";
        RunContext runContext = runContext(tenantId, url, authorEmail, authorName, branch, namespace, gitDirectory);

        String firstFilePath = "first-file.txt";
        String firstFileContent = "First file";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + firstFilePath), new ByteArrayInputStream(firstFileContent.getBytes()));
        String secondFilePath = "nested/second-file.txt";
        String secondFileContent = "Second file";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + secondFilePath), new ByteArrayInputStream(secondFileContent.getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .authorName("{{name}}")
            .namespace("{{namespace}}")
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(url)
                .username(pat)
                .password(pat)
                .branch(branch)
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

            assertThat(pushOutput.getCommitURL(), is(url + "/commit/" + pushOutput.getCommitId()));

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
            assertAuthor(revCommit, authorEmail, authorName);
        } finally {
            this.deleteRemoteBranch(runContext.tempDir(), branch);
        }
    }

    @Test
    void defaultCase_MultipleRegex() throws Exception {
        String tenantId = "my-tenant";
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";
        String authorEmail = "bmulier@kestra.io";
        String authorName = "brianmulier";
        String url = "https://github.com/kestra-io/unit-tests";
        RunContext runContext = runContext(tenantId, url, authorEmail, authorName, branch, namespace, gitDirectory);

        String firstFilePath = "first-file.txt";
        String firstFileContent = "First file";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + firstFilePath), new ByteArrayInputStream(firstFileContent.getBytes()));
        String secondFilePath = "nested/second-file.txt";
        String secondFileContent = "Second file";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + secondFilePath), new ByteArrayInputStream(secondFileContent.getBytes()));
        String thirdFilePath = "third-file.txt";
        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/" + thirdFilePath), new ByteArrayInputStream("Third file".getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .authorEmail("{{email}}")
            .authorName("{{name}}")
            .namespace("{{namespace}}")
            .files(List.of("first*", "second*"))
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(url)
                .username(pat)
                .password(pat)
                .branch(branch)
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

            assertThat(pushOutput.getCommitURL(), is(url + "/commit/" + pushOutput.getCommitId()));

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
            assertAuthor(revCommit, authorEmail, authorName);
        } finally {
            this.deleteRemoteBranch(runContext.tempDir(), branch);
        }
    }

    @Test
    void defaultCase_NoRegexNoAuthor() throws Exception {
        String tenantId = "my-tenant";
        String namespace = IdUtils.create().toLowerCase();
        String branch = IdUtils.create();
        String gitDirectory = "my-files";
        String url = "https://github.com/kestra-io/unit-tests";
        RunContext runContext = runContext(tenantId, url, "", "", branch, namespace, gitDirectory);

        storage.put(tenantId, URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/first-file.txt"), new ByteArrayInputStream("First file".getBytes()));

        PushNamespaceFiles pushNamespaceFiles = PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .branch("{{branch}}")
            .url("{{url}}")
            .commitMessage("Push from CI - {{description}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .namespace("{{namespace}}")
            .gitDirectory("{{gitDirectory}}")
            .build();

        try {
            PushNamespaceFiles.Output pushOutput = pushNamespaceFiles.run(runContext);

            Clone clone = Clone.builder()
                .id("clone")
                .type(Clone.class.getName())
                .url(url)
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
            this.deleteRemoteBranch(runContext.tempDir(), branch);
        }
    }

    private RunContext runContext(String tenantId, String url, String authorEmail, String authorName, String branch, String namespace, String gitDirectory) {
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
            "namespace", namespace,
            "gitDirectory", gitDirectory
        ));
    }

    private static RevCommit assertIsLastCommit(RunContext cloneRunContext, PushNamespaceFiles.Output pushOutput) throws IOException, GitAPIException {
        RevCommit revCommit;
        try (Git git = Git.open(cloneRunContext.tempDir().toFile())) {
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
                new TypeReference<Map<String, String>>() {
                }
            )))
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
