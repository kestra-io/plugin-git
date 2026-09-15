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

        RunContext runContext = runContextFactory.of(
            Map.of(
                "flow", Map.of("tenantId", TENANT_ID, "namespace", "system"),
                "url", repoDir.toUri().toString(),
                "pat", "",
                "branch", "master", // git init default
                "namespace", NAMESPACE,
                "gitDirectory", specialGitDir
            )
        );

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
