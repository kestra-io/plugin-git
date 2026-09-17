package io.kestra.plugin.git;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.Namespace;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.KestraIgnore;
import io.kestra.core.utils.Rethrow;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.git.shared.testkit.AbstractGitTest;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class SyncNamespaceFilesTest extends AbstractGitTest {
    public static final String BRANCH = "sync";
    public static final String GIT_DIRECTORY = "to_clone";
    public static final String TENANT_ID = TenantService.MAIN_TENANT;
    public static final String NAMESPACE = "my.namespace";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private DispatchQueueInterface<LogEntry> logQueue;

    @BeforeEach
    void init() throws IOException {
        runContextFactory.of().storage().namespace(NAMESPACE).delete(Path.of("/"));
    }

    @Test
    void defaultCase_WithDelete() throws Exception {
        RunContext runContext = runContext();

        // not in `gitDirectory` so it should be deleted
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of("README.md"),
            new ByteArrayInputStream("README content".getBytes())
        );
        // will be deleted as it's not on git
        String deletedFilePath = "file_to_delete.txt";
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of(deletedFilePath),
            new ByteArrayInputStream(new byte[0])
        );
        String deletedDirPath = "dir_to_delete";
        runContext.storage().namespace(NAMESPACE).createDirectory(Path.of(deletedDirPath + "/"));

        String deletedDirSubFilePath = "dir_to_delete/file_to_delete.txt";
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of(deletedDirSubFilePath),
            new ByteArrayInputStream(new byte[0])
        );
        // will get updated
        String clonedFilePath = "cloned.json";
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of(clonedFilePath),
            new ByteArrayInputStream("{\"old-field\": \"old-value\"}".getBytes())
        );

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .delete(Property.ofValue(true))
            .build();
        SyncNamespaceFiles.Output syncOutput = task.run(runContext);

        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(KestraIgnore.KESTRA_IGNORE_FILE_NAME)), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("file_to_ignore.txt")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("dir_to_ignore/file.txt")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("dir_to_ignore")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("/_flows/first-flow.yml")), is(true));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("README.md")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(deletedFilePath)), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(deletedDirSubFilePath)), is(false));
        assertNamespaceFileContent(runContext, clonedFilePath, "{\"my-field\": \"my-value\"}");

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(true));
    }

    @Test
    void defaultCase_WithoutDelete() throws Exception {
        RunContext runContext = runContext();

        // not in `gitDirectory` so it should be deleted but since delete flag is false it won't
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of("README.md"),
            new ByteArrayInputStream("README content".getBytes())
        );
        // will not be deleted as it's not on git but delete flag is false
        String deletedFilePath = "file_to_delete.txt";
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of(deletedFilePath),
            new ByteArrayInputStream(new byte[0])
        );
        String deletedDirPath = "dir_to_delete";
        runContext.storage().namespace(NAMESPACE).createDirectory(Path.of(deletedDirPath));

        String deletedDirSubFilePath = "/dir_to_delete/file_to_delete.txt";
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of(deletedDirSubFilePath),
            new ByteArrayInputStream(new byte[0])
        );
        // will get updated
        String clonedFilePath = "/cloned.json";
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of(clonedFilePath),
            new ByteArrayInputStream("{\"old-field\": \"old-value\"}".getBytes())
        );

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .build();
        SyncNamespaceFiles.Output syncOutput = task.run(runContext);

        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(KestraIgnore.KESTRA_IGNORE_FILE_NAME)), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("file_to_ignore.txt")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("dir_to_ignore/file.txt")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("dir_to_ignore")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("_flows/first-flow.yml")), is(true));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("README.md")), is(true));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(deletedFilePath)), is(true));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(deletedDirPath)), is(true));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(deletedDirSubFilePath)), is(true));
        assertNamespaceFileContent(runContext, clonedFilePath, "{\"my-field\": \"my-value\"}");

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(false));
    }

    @Test
    void defaultCase_DryRunWithDeleteFlag_ShouldStillNotifyWhatWouldBeDeleted() throws Exception {
        RunContext runContext = runContext();

        // not in `gitDirectory` so it should be deleted
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of("README.md"),
            new ByteArrayInputStream("README content".getBytes())
        );
        // will be deleted as it's not on git
        String deletedFilePath = "file_to_delete.txt";
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of(deletedFilePath),
            new ByteArrayInputStream(new byte[0])
        );
        String deletedDirPath = "dir_to_delete";
        runContext.storage().namespace(NAMESPACE).createDirectory(Path.of(deletedDirPath));

        String deletedDirSubFilePath = "dir_to_delete/file_to_delete.txt";
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of(deletedDirSubFilePath),
            new ByteArrayInputStream(new byte[0])
        );
        // will get updated
        String clonedFilePath = "cloned.json";
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of(clonedFilePath),
            new ByteArrayInputStream("{\"old-field\": \"old-value\"}".getBytes())
        );

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .dryRun(Property.ofValue(true))
            .delete(Property.ofValue(true))
            .build();
        SyncNamespaceFiles.Output syncOutput = task.run(runContext);

        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(KestraIgnore.KESTRA_IGNORE_FILE_NAME)), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("file_to_ignore.txt")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("dir_to_ignore/file.txt")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("dir_to_ignore")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("_flows/first-flow.yml")), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("README.md")), is(true));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(deletedFilePath)), is(true));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(deletedDirPath)), is(true));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(deletedDirSubFilePath)), is(true));
        assertNamespaceFileContent(runContext, clonedFilePath, "{\"old-field\": \"old-value\"}");

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(true));
    }

    @Test
    void secondSyncWithoutGitChanges_ShouldReportUnchangedFiles() throws Exception {
        var task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .build();

        task.run(runContext());
        var secondRunContext = runContext();
        var secondSyncOutput = task.run(secondRunContext);

        var secondDiffs = readDiffs(secondRunContext, secondSyncOutput.diffFileUri());
        assertThat(secondDiffs, hasSize(defaultCaseDiffs(false).size()));
        assertThat(secondDiffs.stream().map(diff -> diff.get("syncState")).toList(), not(hasItem("OVERWRITTEN")));
        assertThat(
            secondDiffs.stream()
                .filter(diff -> "to_clone/cloned.json".equals(diff.get("gitPath")))
                .findFirst()
                .orElseThrow()
                .get("syncState"),
            is("UNCHANGED")
        );
    }

    /**
     * unit test to reproduce <a href="https://github.com/kestra-io/kestra-ee/issues/6402">...</a>
     */
    @Test
    void syncWithDotGitInDirectoryName_ShouldSyncFiles() throws Exception {
        String specialGitDir = "to_clone.github_files";
        String expectedFile = "hello.txt";

        Path repoDir = Files.createTempDirectory("unit-test.special-dir-repo");
        try (
            org.eclipse.jgit.api.Git git = org.eclipse.jgit.api.Git.init()
                .setDirectory(repoDir.toFile())
                .call()
        ) {

            Path specialDir = repoDir.resolve(specialGitDir);
            Files.createDirectories(specialDir);
            Files.writeString(specialDir.resolve(expectedFile), "hello");

            git.add().addFilepattern(".").call();
            git.commit()
                .setMessage("test commit")
                .setAuthor("test", "test@test.com")
                .call();
        }

        RunContext runContext = localRepoRunContext(repoDir, specialGitDir);

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .build();

        task.run(runContext);

        assertThat(
            runContext.storage().namespace(NAMESPACE).exists(Path.of(expectedFile)),
            is(true)
        );

    }

    /**
     * Reproduces <a href="https://github.com/kestra-io/plugin-git/issues/338">#338</a>: syncing into a namespace
     * that doesn't exist yet must create it so the files are visible in the UI, not just written to storage.
     */
    @Test
    void namespaceMissing_ShouldBeCreatedAutomatically() throws Exception {
        String targetNamespace = "new.namespace.missing";
        runContextFactory.of().storage().namespace(targetNamespace).delete(Path.of("/"));

        try (var apiServer = new NamespaceLifecycleMockServer()) {
            RunContext runContext = runContextFactory.of(
                Map.of(
                    "flow", Map.of("tenantId", TENANT_ID, "namespace", "system"),
                    "url", repositoryUrl,
                    "pat", pat,
                    "branch", BRANCH,
                    "namespace", targetNamespace,
                    "gitDirectory", GIT_DIRECTORY
                )
            );

            SyncNamespaceFiles task = SyncNamespaceFiles.builder()
                .url(Property.ofExpression("{{url}}"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .branch(Property.ofExpression("{{branch}}"))
                .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
                .namespace(Property.ofExpression("{{namespace}}"))
                .kestraUrl(Property.ofValue(apiServer.url()))
                .build();

            task.run(runContext);

            assertThat(apiServer.createAttempts(), is(List.of(targetNamespace)));
            assertThat(apiServer.createdNamespaces(), is(List.of(targetNamespace)));
            assertThat(runContext.storage().namespace(targetNamespace).exists(Path.of("/_flows/first-flow.yml")), is(true));
        }
    }

    /**
     * {@code GET namespaces/{id}} returns a synthetic 200 even for a namespace that was never persisted, so
     * existence can only be established by attempting the create and treating an "already exists" response as a
     * no-op. Kestra Enterprise Edition reports that as a {@code 422} validation error, not a {@code 409}; this
     * must be recognized as a benign conflict, not surfaced as a misleading warning on every re-sync.
     */
    @Test
    void namespaceAlreadyExists_ShouldSwallowConflict() throws Exception {
        String targetNamespace = "existing.namespace";
        runContextFactory.of().storage().namespace(targetNamespace).delete(Path.of("/"));

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        logQueue.addListener(logs::add);

        try (var apiServer = new NamespaceLifecycleMockServer(targetNamespace)) {
            SyncNamespaceFiles task = SyncNamespaceFiles.builder()
                .id("sync-namespace-files-existing")
                .type(SyncNamespaceFiles.class.getName())
                .url(Property.ofValue(repositoryUrl))
                .username(Property.ofValue(pat))
                .password(Property.ofValue(pat))
                .branch(Property.ofValue(BRANCH))
                .gitDirectory(Property.ofValue(GIT_DIRECTORY))
                .namespace(Property.ofValue(targetNamespace))
                .kestraUrl(Property.ofValue(apiServer.url()))
                .build();

            RunContext runContext = TestsUtils.mockRunContext(TENANT_ID, runContextFactory, task, Collections.emptyMap());
            task.run(runContext);

            assertThat(apiServer.createAttempts(), is(List.of(targetNamespace)));
            assertThat(apiServer.createdNamespaces(), empty());
            assertThat(runContext.storage().namespace(targetNamespace).exists(Path.of("/_flows/first-flow.yml")), is(true));

            List<LogEntry> warnLogs = TestsUtils.awaitLogs(
                logs,
                logEntry -> logEntry.getLevel().equals(Level.WARN) && logEntry.getMessage() != null && logEntry.getMessage().contains(targetNamespace),
                1
            );
            assertThat(warnLogs, empty());
        }
    }

    @Test
    void dryRun_ShouldNotCreateNamespace() throws Exception {
        String targetNamespace = "dryrun.namespace.missing";
        runContextFactory.of().storage().namespace(targetNamespace).delete(Path.of("/"));

        try (var apiServer = new NamespaceLifecycleMockServer()) {
            RunContext runContext = runContextFactory.of(
                Map.of(
                    "flow", Map.of("tenantId", TENANT_ID, "namespace", "system"),
                    "url", repositoryUrl,
                    "pat", pat,
                    "branch", BRANCH,
                    "namespace", targetNamespace,
                    "gitDirectory", GIT_DIRECTORY
                )
            );

            SyncNamespaceFiles task = SyncNamespaceFiles.builder()
                .url(Property.ofExpression("{{url}}"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .branch(Property.ofExpression("{{branch}}"))
                .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
                .namespace(Property.ofExpression("{{namespace}}"))
                .kestraUrl(Property.ofValue(apiServer.url()))
                .dryRun(Property.ofValue(true))
                .build();

            task.run(runContext);

            assertThat(apiServer.createAttempts(), empty());
            assertThat(apiServer.createdNamespaces(), empty());
        }
    }

    /**
     * Backward-compatibility guard: if the Kestra API is unreachable (e.g. no {@code auth} configured and no
     * server at the default URL), the sync must still succeed instead of failing the task.
     */
    @Test
    void kestraApiUnreachable_ShouldWarnAndStillSync() throws Exception {
        String targetNamespace = "unreachable.namespace";
        runContextFactory.of().storage().namespace(targetNamespace).delete(Path.of("/"));

        String unreachableUrl;
        try (var apiServer = new NamespaceLifecycleMockServer()) {
            unreachableUrl = apiServer.url();
        }

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        logQueue.addListener(logs::add);

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .id("sync-namespace-files")
            .type(SyncNamespaceFiles.class.getName())
            .url(Property.ofValue(repositoryUrl))
            .username(Property.ofValue(pat))
            .password(Property.ofValue(pat))
            .branch(Property.ofValue(BRANCH))
            .gitDirectory(Property.ofValue(GIT_DIRECTORY))
            .namespace(Property.ofValue(targetNamespace))
            .kestraUrl(Property.ofValue(unreachableUrl))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(TENANT_ID, runContextFactory, task, Collections.emptyMap());
        SyncNamespaceFiles.Output output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(runContext.storage().namespace(targetNamespace).exists(Path.of("/_flows/first-flow.yml")), is(true));

        List<LogEntry> warnLogs = TestsUtils.awaitLogs(
            logs,
            logEntry -> logEntry.getLevel().equals(Level.WARN) && logEntry.getMessage().contains(targetNamespace),
            1
        );
        assertThat(warnLogs, hasSize(1));
    }

    /**
     * unit test to reproduce <a href="https://github.com/kestra-io/plugin-git/issues/330">...</a>:
     * a file removed from Git between two syncs must be deleted from the namespace on the second sync.
     */
    @Test
    void secondSyncAfterFileRemovedFromGit_WithDelete_ShouldDeleteFile() throws Exception {
        String gitDir = "namespace_files";
        String keptFile = "keep.txt";
        String removedFile = "to_delete.txt";

        Path repoDir = Files.createTempDirectory("unit-test.delete-on-second-sync-repo");
        Path filesDir = repoDir.resolve(gitDir);
        Files.createDirectories(filesDir);
        Files.writeString(filesDir.resolve(keptFile), "keep me");
        Files.writeString(filesDir.resolve(removedFile), "delete me");

        try (org.eclipse.jgit.api.Git git = org.eclipse.jgit.api.Git.init().setDirectory(repoDir.toFile()).call()) {
            git.add().addFilepattern(".").call();
            git.commit().setMessage("initial commit").setAuthor("test", "test@test.com").call();

            SyncNamespaceFiles task = SyncNamespaceFiles.builder()
                .url(Property.ofExpression("{{url}}"))
                .branch(Property.ofExpression("{{branch}}"))
                .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
                .namespace(Property.ofExpression("{{namespace}}"))
                .delete(Property.ofValue(true))
                .build();

            RunContext firstRunContext = localRepoRunContext(repoDir, gitDir);
            task.run(firstRunContext);

            assertThat(firstRunContext.storage().namespace(NAMESPACE).exists(Path.of(keptFile)), is(true));
            assertThat(firstRunContext.storage().namespace(NAMESPACE).exists(Path.of(removedFile)), is(true));

            Files.delete(filesDir.resolve(removedFile));
            git.rm().addFilepattern(gitDir + "/" + removedFile).call();
            git.commit().setMessage("remove file").setAuthor("test", "test@test.com").call();

            RunContext secondRunContext = localRepoRunContext(repoDir, gitDir);
            SyncNamespaceFiles.Output secondSyncOutput = task.run(secondRunContext);

            assertThat(secondRunContext.storage().namespace(NAMESPACE).exists(Path.of(keptFile)), is(true));
            assertThat(secondRunContext.storage().namespace(NAMESPACE).exists(Path.of(removedFile)), is(false));

            List<Map<String, String>> secondDiffs = readDiffs(secondRunContext, secondSyncOutput.diffFileUri());
            String expectedKestraPath = "/" + NAMESPACE.replace('.', '/') + "/_files/" + removedFile;
            assertThat(
                secondDiffs.stream()
                    .filter(diff -> expectedKestraPath.equals(diff.get("kestraPath")))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(removedFile + " not reported as DELETED in second sync diffs: " + secondDiffs))
                    .get("syncState"),
                is("DELETED")
            );
        }
    }

    @Test
    void namespaceDirectory_DefaultIsNoOp() throws Exception {
        Path repoDir = createLocalRepo(Map.of("content/hello.txt", "hello"));

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofValue("content"))
            .build();
        task.run(localRunContext(repoDir));

        assertThat(runContext().storage().namespace(NAMESPACE).exists(Path.of("hello.txt")), is(true));
    }

    @Test
    void namespaceDirectory_PrefixesDestinationPath() throws Exception {
        Path repoDir = createLocalRepo(Map.of("content/foo.py", "print(1)"));

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofValue("content"))
            .namespaceDirectory(Property.ofValue("/shared-scripts"))
            .build();
        task.run(localRunContext(repoDir));

        assertNamespaceFileContent(runContext(), "shared-scripts/foo.py", "print(1)");
        assertThat(runContext().storage().namespace(NAMESPACE).exists(Path.of("foo.py")), is(false));
    }

    @Test
    void namespaceDirectory_DryRun_ReportsPrefixedKestraPath() throws Exception {
        Path repoDir = createLocalRepo(Map.of("content/foo.py", "print(1)"));

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofValue("content"))
            .namespaceDirectory(Property.ofValue("/shared-scripts"))
            .dryRun(Property.ofValue(true))
            .build();
        RunContext runContext = localRunContext(repoDir);
        SyncNamespaceFiles.Output output = task.run(runContext);

        List<Map<String, String>> diffs = readDiffs(runContext, output.diffFileUri());
        assertThat(
            diffs.stream().map(diff -> diff.get("kestraPath")).toList(),
            hasItem("/my/namespace/_files/shared-scripts/foo.py")
        );
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of("shared-scripts/foo.py")), is(false));
    }

    @Test
    void namespaceDirectory_Delete_LeavesFilesOutsidePrefixUntouched() throws Exception {
        Path repoDir = createLocalRepo(Map.of("content/keep.txt", "kept from git"));

        RunContext setupContext = runContext();
        setupContext.storage().namespace(NAMESPACE).putFile(Path.of("outside.txt"), new ByteArrayInputStream("outside prefix".getBytes()));
        setupContext.storage().namespace(NAMESPACE).putFile(Path.of("shared-scripts/stale.txt"), new ByteArrayInputStream("stale".getBytes()));

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofValue("content"))
            .namespaceDirectory(Property.ofValue("/shared-scripts"))
            .delete(Property.ofValue(true))
            .build();
        task.run(localRunContext(repoDir));

        Namespace namespace = runContext().storage().namespace(NAMESPACE);
        assertThat(namespace.exists(Path.of("outside.txt")), is(true));
        assertThat(namespace.exists(Path.of("shared-scripts/stale.txt")), is(false));
        assertNamespaceFileContent(runContext(), "shared-scripts/keep.txt", "kept from git");
    }

    @Test
    void namespaceDirectory_Delete_EmptyGitSubtree_RemovesOnlyInPrefixFiles() throws Exception {
        // .gitkeep is filtered out of sync content (see isGitInternalPath), so the repo is effectively empty
        Path repoDir = createLocalRepo(Map.of("content/.gitkeep", ""));

        RunContext setupContext = runContext();
        setupContext.storage().namespace(NAMESPACE).putFile(Path.of("outside.txt"), new ByteArrayInputStream("outside prefix".getBytes()));
        setupContext.storage().namespace(NAMESPACE).putFile(Path.of("shared-scripts/a.txt"), new ByteArrayInputStream("a".getBytes()));
        setupContext.storage().namespace(NAMESPACE).putFile(Path.of("shared-scripts/nested/b.txt"), new ByteArrayInputStream("b".getBytes()));

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofValue("content"))
            .namespaceDirectory(Property.ofValue("/shared-scripts"))
            .delete(Property.ofValue(true))
            .build();
        task.run(localRunContext(repoDir));

        Namespace namespace = runContext().storage().namespace(NAMESPACE);
        assertThat(namespace.exists(Path.of("outside.txt")), is(true));
        assertThat(namespace.exists(Path.of("shared-scripts/a.txt")), is(false));
        assertThat(namespace.exists(Path.of("shared-scripts/nested/b.txt")), is(false));
    }

    @Test
    void namespaceDirectory_Delete_ChildNamespace_ScopesToPrefix() throws Exception {
        String childNamespace = NAMESPACE + ".child";
        runContextFactory.of().storage().namespace(childNamespace).delete(Path.of("/"));

        Path repoDir = createLocalRepo(Map.of("content/" + childNamespace + "/keep.txt", "kept from git"));

        RunContext setupContext = runContext();
        setupContext.storage().namespace(childNamespace).putFile(Path.of("outside.txt"), new ByteArrayInputStream("outside prefix".getBytes()));
        setupContext.storage().namespace(childNamespace).putFile(Path.of("shared-scripts/stale.txt"), new ByteArrayInputStream("stale".getBytes()));

        try (NamespacesSearchMockServer namespaces = NamespacesSearchMockServer.start(List.of(childNamespace))) {
            SyncNamespaceFiles task = SyncNamespaceFiles.builder()
                .url(Property.ofExpression("{{url}}"))
                .branch(Property.ofExpression("{{branch}}"))
                .namespace(Property.ofExpression("{{namespace}}"))
                .gitDirectory(Property.ofValue("content"))
                .namespaceDirectory(Property.ofValue("/shared-scripts"))
                .includeChildNamespaces(Property.ofValue(true))
                .delete(Property.ofValue(true))
                .kestraUrl(Property.ofValue(namespaces.url()))
                .build();
            task.run(localRunContext(repoDir));
        }

        Namespace childStorage = runContext().storage().namespace(childNamespace);
        assertThat(childStorage.exists(Path.of("outside.txt")), is(true));
        assertThat(childStorage.exists(Path.of("shared-scripts/stale.txt")), is(false));
        try (InputStream is = childStorage.getFileContent(Path.of("shared-scripts/keep.txt"))) {
            assertThat(new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.joining("\n")), is("kept from git"));
        }
    }

    @Test
    void namespaceDirectory_AcceptsWithAndWithoutLeadingSlash() throws Exception {
        Path repoDir = createLocalRepo(Map.of("content/foo.py", "print(1)"));

        SyncNamespaceFiles withoutLeadingSlash = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofValue("content"))
            .namespaceDirectory(Property.ofValue("shared-scripts"))
            .build();
        withoutLeadingSlash.run(localRunContext(repoDir));
        assertThat(runContext().storage().namespace(NAMESPACE).exists(Path.of("shared-scripts/foo.py")), is(true));

        runContext().storage().namespace(NAMESPACE).delete(Path.of("/"));

        SyncNamespaceFiles withTrailingSlash = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofValue("content"))
            .namespaceDirectory(Property.ofValue("/shared-scripts/"))
            .build();
        withTrailingSlash.run(localRunContext(repoDir));
        assertThat(runContext().storage().namespace(NAMESPACE).exists(Path.of("shared-scripts/foo.py")), is(true));
    }

    @Test
    void namespaceDirectory_RejectsPathTraversal() throws Exception {
        Path repoDir = createLocalRepo(Map.of("content/foo.py", "print(1)"));

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofValue("content"))
            .namespaceDirectory(Property.ofValue("../escape"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(localRunContext(repoDir)));
    }

    @Test
    void namespaceDirectory_RejectsPercentEncodedPathTraversal() throws Exception {
        Path repoDir = createLocalRepo(Map.of("content/foo.py", "print(1)"));

        // %2e%2e decodes back to `..` once the prefix is concatenated into a URI in resolveTarget, so it must be
        // rejected just like a literal `..` — otherwise the destination path could escape the namespace subtree.
        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
            .url(Property.ofExpression("{{url}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .gitDirectory(Property.ofValue("content"))
            .namespaceDirectory(Property.ofValue("/%2e%2e/escape"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(localRunContext(repoDir)));
    }

    private Path createLocalRepo(Map<String, String> filesByRelativePath) throws Exception {
        Path repoDir = Files.createTempDirectory("unit-test.namespace-directory-repo");
        try (Git git = Git.init().setDirectory(repoDir.toFile()).call()) {
            for (Map.Entry<String, String> entry : filesByRelativePath.entrySet()) {
                Path filePath = repoDir.resolve(entry.getKey());
                Files.createDirectories(filePath.getParent());
                Files.writeString(filePath, entry.getValue());
            }
            git.add().addFilepattern(".").call();
            git.commit().setMessage("test commit").setAuthor("test", "test@test.com").call();
        }
        return repoDir;
    }

    private RunContext localRunContext(Path repoDir) {
        return runContextFactory.of(
            Map.of(
                "flow", Map.of("tenantId", TENANT_ID, "namespace", "system"),
                "url", repoDir.toUri().toString(),
                "pat", "",
                "branch", "master", // git init default
                "namespace", NAMESPACE
            )
        );
    }

    private static List<Map<String, String>> defaultCaseDiffs(boolean withDeleted) {
        ArrayList<Map<String, String>> diffs = new ArrayList<>(
            List.of(
                Map.of("gitPath", "to_clone/_flows/", "syncState", "ADDED", "kestraPath", "/my/namespace/_files/_flows/"),
                Map.of("gitPath", "to_clone/_flows/nested/", "syncState", "ADDED", "kestraPath", "/my/namespace/_files/_flows/nested/"),
                Map.of("gitPath", "to_clone/_flows/nested/namespace/", "syncState", "ADDED", "kestraPath", "/my/namespace/_files/_flows/nested/namespace/"),
                Map.of("gitPath", "to_clone/_flows/nested/namespace/nested_flow.yaml", "syncState", "ADDED", "kestraPath", "/my/namespace/_files/_flows/nested/namespace/nested_flow.yaml"),
                Map.of("gitPath", "to_clone/_flows/first-flow.yml", "syncState", "ADDED", "kestraPath", "/my/namespace/_files/_flows/first-flow.yml"),
                Map.of("gitPath", "to_clone/_flows/unchanged-flow.yaml", "syncState", "ADDED", "kestraPath", "/my/namespace/_files/_flows/unchanged-flow.yaml"),
                Map.of("gitPath", "to_clone/_flows/.kestraignore", "syncState", "ADDED", "kestraPath", "/my/namespace/_files/_flows/.kestraignore"),
                Map.of("gitPath", "to_clone/_flows/kestra-ignored-flow.yml", "syncState", "ADDED", "kestraPath", "/my/namespace/_files/_flows/kestra-ignored-flow.yml"),
                Map.of("gitPath", "to_clone/_flows/second-flow.yml", "syncState", "ADDED", "kestraPath", "/my/namespace/_files/_flows/second-flow.yml"),
                Map.of("gitPath", "to_clone/cloned.json", "syncState", "OVERWRITTEN", "kestraPath", "/my/namespace/_files/cloned.json")
            )
        );

        if (withDeleted) {
            diffs.addAll(
                List.of(
                    new HashMap<>() {
                        {
                            this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/my/namespace/_files/file_to_delete.txt"));
                            this.put("gitPath", null);
                        }
                    },
                    new HashMap<>() {
                        {
                            this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/my/namespace/_files/dir_to_delete/file_to_delete.txt"));
                            this.put("gitPath", null);
                        }
                    },
                    new HashMap<>() {
                        {
                            this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/my/namespace/_files/README.md"));
                            this.put("gitPath", null);
                        }
                    }
                )
            );
        }
        return diffs;
    }

    private RunContext localRepoRunContext(Path repoDir, String gitDir) {
        return runContextFactory.of(
            Map.of(
                "flow", Map.of("tenantId", TENANT_ID, "namespace", "system"),
                "url", repoDir.toUri().toString(),
                "pat", "",
                "branch", "master",
                "namespace", NAMESPACE,
                "gitDirectory", gitDir
            )
        );
    }

    private RunContext runContext() {
        return runContextFactory.of(
            Map.of(
                "flow", Map.of(
                    "tenantId", SyncNamespaceFilesTest.TENANT_ID,
                    "namespace", "system"
                ),
                "url", repositoryUrl,
                "pat", pat,
                "branch", SyncNamespaceFilesTest.BRANCH,
                "namespace", SyncNamespaceFilesTest.NAMESPACE,
                "gitDirectory", SyncNamespaceFilesTest.GIT_DIRECTORY
            )
        );
    }

    private static void assertDiffs(RunContext runContext, URI diffFileUri, List<Map<String, String>> expectedDiffs) throws IOException {
        List<Map<String, String>> diffMaps = readDiffs(runContext, diffFileUri);
        assertThat(diffMaps, containsInAnyOrder(expectedDiffs.toArray(Map[]::new)));
    }

    private static List<Map<String, String>> readDiffs(RunContext runContext, URI diffFileUri) throws IOException {
        String diffSummary = IOUtils.toString(runContext.storage().getFile(diffFileUri), StandardCharsets.UTF_8);
        return diffSummary.lines()
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
    }

    private void assertNamespaceFileContent(RunContext runContext, String namespaceFileUri, String expectedFileContent) throws IOException {
        Namespace namespace = runContext.storage().namespace(NAMESPACE);
        try (InputStream is = namespace.getFileContent(Path.of(namespaceFileUri))) {
            assertThat(new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.joining("\n")), is(expectedFileContent));
        }
    }
}
