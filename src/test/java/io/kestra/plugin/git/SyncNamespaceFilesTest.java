package io.kestra.plugin.git;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.KestraIgnore;
import io.kestra.core.utils.Rethrow;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
public class SyncNamespaceFilesTest {
    public static final String BRANCH = "reconcile";
    public static final String GIT_DIRECTORY = "to_clone";
    public static final String TENANT_ID = "my-tenant";
    public static final String NAMESPACE = "my.namespace";
    public static final String URL = "https://github.com/kestra-io/unit-tests";

    @Value("${kestra.git.pat}")
    private String pat;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storage;

    @Inject
    private QueueInterface<LogEntry> logQueue;

    @BeforeEach
    void init() throws IOException {
        storage.deleteByPrefix(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE)));
    }

    @Test
    void hardcodedPassword() {
        SyncNamespaceFiles syncNamespaceFiles = SyncNamespaceFiles.builder()
                .id("syncNamespaceFiles")
                .type(PushNamespaceFiles.class.getName())
                .url(URL)
                .password("my-password")
                .build();

        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> syncNamespaceFiles.run(runContextFactory.of(Map.of(
                "flow", Map.of(
                        "tenantId", "tenantId",
                        "namespace", "system"
                ))))
        );
        assertThat(illegalArgumentException.getMessage(), is("It looks like you're trying to push a flow with a hard-coded Git credential. Make sure to pass the credential securely using a Pebble expression (e.g. using secrets or environment variables)."));
    }

    @Test
    void defaultCase_WithDelete() throws Exception {
        RunContext runContext = runContext();

        // not in `gitDirectory` so it should be deleted
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md"),
                new ByteArrayInputStream("README content".getBytes())
        );
        // will be deleted as it's not on git
        String deletedFilePath = "/file_to_delete.txt";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath),
                new ByteArrayInputStream(new byte[0])
        );
        String deletedDirPath = "/dir_to_delete";
        storage.createDirectory(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)
        );
        String deletedDirSubFilePath = "/dir_to_delete/file_to_delete.txt";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath),
                new ByteArrayInputStream(new byte[0])
        );
        // will get updated
        String clonedFilePath = "/cloned.json";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + clonedFilePath),
                new ByteArrayInputStream("{\"old-field\": \"old-value\"}".getBytes())
        );

        // check behaviour in case of converting a file to dir or dir to file
        String fileToDirPath = "/file_to_dir";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + fileToDirPath),
                new ByteArrayInputStream("some content".getBytes())
        );

        String dirToFilePath = "/dir_to_file";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + dirToFilePath + "/file.txt"),
                new ByteArrayInputStream("nested file content".getBytes())
        );

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
                .url("{{url}}")
                .username("{{pat}}")
                .password("{{pat}}")
                .branch("{{branch}}")
                .gitDirectory("{{gitDirectory}}")
                .namespace("{{namespace}}")
                .delete(true)
                .build();
        SyncNamespaceFiles.Output syncOutput = task.run(runContext);

        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/" + KestraIgnore.KESTRA_IGNORE_FILE_NAME)), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/file_to_ignore.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/dir_to_ignore/file.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/dir_to_ignore")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/_flows/first-flow.yml")), is(true));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath)), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath)), is(false));
        assertNamespaceFileContent(clonedFilePath, "{\"my-field\": \"my-value\"}");
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + fileToDirPath)), is(true));
        assertNamespaceFileContent(fileToDirPath + "/file.txt", "directory replacing file");
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + dirToFilePath + "/file.txt")), is(false));
        assertNamespaceFileContent(dirToFilePath, "file replacing a directory");

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(true));
    }

    @Test
    void defaultCase_WithoutDelete() throws Exception {
        RunContext runContext = runContext();

        // not in `gitDirectory` so it should be deleted but since delete flag is false it won't
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md"),
                new ByteArrayInputStream("README content".getBytes())
        );
        // will not be deleted as it's not on git but delete flag is false
        String deletedFilePath = "/file_to_delete.txt";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath),
                new ByteArrayInputStream(new byte[0])
        );
        String deletedDirPath = "/dir_to_delete";
        storage.createDirectory(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)
        );
        String deletedDirSubFilePath = "/dir_to_delete/file_to_delete.txt";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath),
                new ByteArrayInputStream(new byte[0])
        );
        // will get updated
        String clonedFilePath = "/cloned.json";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + clonedFilePath),
                new ByteArrayInputStream("{\"old-field\": \"old-value\"}".getBytes())
        );

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
                .url("{{url}}")
                .username("{{pat}}")
                .password("{{pat}}")
                .branch("{{branch}}")
                .gitDirectory("{{gitDirectory}}")
                .namespace("{{namespace}}")
                .build();
        SyncNamespaceFiles.Output syncOutput = task.run(runContext);

        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/" + KestraIgnore.KESTRA_IGNORE_FILE_NAME)), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/file_to_ignore.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/dir_to_ignore/file.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/dir_to_ignore")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/_flows/first-flow.yml")), is(true));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md")), is(true));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath)), is(true));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)), is(true));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath)), is(true));
        assertNamespaceFileContent(clonedFilePath, "{\"my-field\": \"my-value\"}");

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(false));
    }

    @Test
    void defaultCase_DryRunWithDeleteFlag_ShouldStillNotifyWhatWouldBeDeleted() throws Exception {
        RunContext runContext = runContext();

        // not in `gitDirectory` so it should be deleted
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md"),
                new ByteArrayInputStream("README content".getBytes())
        );
        // will be deleted as it's not on git
        String deletedFilePath = "/file_to_delete.txt";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath),
                new ByteArrayInputStream(new byte[0])
        );
        String deletedDirPath = "/dir_to_delete";
        storage.createDirectory(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)
        );
        String deletedDirSubFilePath = "/dir_to_delete/file_to_delete.txt";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath),
                new ByteArrayInputStream(new byte[0])
        );
        // will get updated
        String clonedFilePath = "/cloned.json";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + clonedFilePath),
                new ByteArrayInputStream("{\"old-field\": \"old-value\"}".getBytes())
        );

        // check behaviour in case of converting a file to dir or dir to file
        String fileToDirPath = "/file_to_dir";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + fileToDirPath),
                new ByteArrayInputStream("some content".getBytes())
        );

        String dirToFilePath = "/dir_to_file";
        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + dirToFilePath + "/file.txt"),
                new ByteArrayInputStream("nested file content".getBytes())
        );

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
                .url("{{url}}")
                .username("{{pat}}")
                .password("{{pat}}")
                .branch("{{branch}}")
                .gitDirectory("{{gitDirectory}}")
                .namespace("{{namespace}}")
                .dryRun(true)
                .delete(true)
                .build();
        SyncNamespaceFiles.Output syncOutput = task.run(runContext);

        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/" + KestraIgnore.KESTRA_IGNORE_FILE_NAME)), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/file_to_ignore.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/dir_to_ignore/file.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/dir_to_ignore")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/_flows/first-flow.yml")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md")), is(true));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath)), is(true));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)), is(true));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath)), is(true));
        assertNamespaceFileContent(clonedFilePath, "{\"old-field\": \"old-value\"}");
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + fileToDirPath)), is(true));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + fileToDirPath + "/file.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + dirToFilePath + "/file.txt")), is(true));

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(true));
    }



    @Test
    void dirToFile_fileToDir_WithoutDelete() throws Exception {
        String fileToDirPath = "/file_to_dir";
        storage.put(
                null,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + fileToDirPath),
                new ByteArrayInputStream("some content".getBytes())
        );

        List<LogEntry> firstSyncLogs = new CopyOnWriteArrayList<>();
        Runnable stopListeningToLogs = logQueue.receive(logWithException -> firstSyncLogs.add(logWithException.getLeft()));
        SyncNamespaceFiles syncNamespaceFiles = SyncNamespaceFiles.builder()
                .id("sync")
                .type(SyncNamespaceFiles.class.getName())
                .url(URL)
                .username("{{inputs.pat}}")
                .password("{{inputs.pat}}")
                .branch(BRANCH)
                .gitDirectory(GIT_DIRECTORY)
                .namespace(NAMESPACE)
                .build();
        Map<String, Object> inputsMap = Map.of("pat", pat);
        syncNamespaceFiles.run(TestsUtils.mockRunContext(runContextFactory, syncNamespaceFiles, inputsMap));

        TestsUtils.awaitLogs(
                firstSyncLogs,
                logEntry -> logEntry.getLevel() == Level.WARN && logEntry.getMessage().equals("Kestra already has a file named /file_to_dir and Git has a directory with the same name. If you want to proceed with file replacement with directory, please add `delete: true` flag."),
                1
        );
        stopListeningToLogs.run();

        String dirToFilePath = "/dir_to_file";
        storage.delete(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + dirToFilePath));

        storage.put(
                TENANT_ID,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + dirToFilePath + "/file.txt"),
                new ByteArrayInputStream("nested file content".getBytes())
        );

        List<LogEntry> secondSyncLogs = new CopyOnWriteArrayList<>();
        stopListeningToLogs = logQueue.receive(logWithException -> secondSyncLogs.add(logWithException.getLeft()));
        syncNamespaceFiles.run(TestsUtils.mockRunContext(runContextFactory, syncNamespaceFiles, inputsMap));

        TestsUtils.awaitLogs(
                secondSyncLogs,
                logEntry -> logEntry.getLevel() == Level.WARN && logEntry.getMessage().equals("Kestra already has a directory under " + dirToFilePath + " and Git has a file with the same name. If you want to proceed with directory replacement with file, please add `delete: true` flag."),
                1
        );
        stopListeningToLogs.run();
    }

    private static List<Map<String, String>> defaultCaseDiffs(boolean withDeleted) {
        ArrayList<Map<String, String>> diffs = new ArrayList<>(List.of(
                Map.of("gitPath", "to_clone/_flows/", "syncState", "ADDED", "kestraPath", "/_flows/"),
                Map.of("gitPath", "to_clone/_flows/fakesub-namespace-flow.yml", "syncState", "ADDED", "kestraPath", "/_flows/fakesub-namespace-flow.yml"),
                Map.of("gitPath", "to_clone/_flows/first-flow.yml", "syncState", "ADDED", "kestraPath", "/_flows/first-flow.yml"),
                Map.of("gitPath", "to_clone/_flows/kestra-ignored-flow.yml", "syncState", "ADDED", "kestraPath", "/_flows/kestra-ignored-flow.yml"),
                Map.of("gitPath", "to_clone/_flows/second-flow.yml", "syncState", "ADDED", "kestraPath", "/_flows/second-flow.yml"),
                Map.of("gitPath", "to_clone/_flows/sub-namespace-flow.yml", "syncState", "ADDED", "kestraPath", "/_flows/sub-namespace-flow.yml"),
                Map.of("gitPath", "to_clone/cloned.json", "syncState", "OVERWRITTEN", "kestraPath", "/cloned.json"),
                Map.of("gitPath", "to_clone/dir_to_file", "syncState", "ADDED", "kestraPath", "/dir_to_file"),
                Map.of("gitPath", "to_clone/file_to_dir/", "syncState", "ADDED", "kestraPath", "/file_to_dir/"),
                Map.of("gitPath", "to_clone/file_to_dir/file.txt", "syncState", "ADDED", "kestraPath", "/file_to_dir/file.txt")
        ));

        if (withDeleted) {
            diffs.addAll(List.of(
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/file_to_dir"));
                        this.put("gitPath", null);
                    }},
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/file_to_delete.txt"));
                        this.put("gitPath", null);
                    }},
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/dir_to_file/file.txt"));
                        this.put("gitPath", null);
                    }},
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/dir_to_file/"));
                        this.put("gitPath", null);
                    }},
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/dir_to_delete/file_to_delete.txt"));
                        this.put("gitPath", null);
                    }},
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/dir_to_delete/"));
                        this.put("gitPath", null);
                    }},
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/README.md"));
                        this.put("gitPath", null);
                    }}
            ));
        }
        return diffs;
    }

    private RunContext runContext() {
        return runContextFactory.of(Map.of(
                "flow", Map.of(
                        "tenantId", SyncNamespaceFilesTest.TENANT_ID,
                        "namespace", "system"
                ),
                "url", SyncNamespaceFilesTest.URL,
                "pat", pat,
                "branch", SyncNamespaceFilesTest.BRANCH,
                "namespace", SyncNamespaceFilesTest.NAMESPACE,
                "gitDirectory", SyncNamespaceFilesTest.GIT_DIRECTORY
        ));
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

    private void assertNamespaceFileContent(String namespaceFileUri, String expectedFileContent) throws IOException {
        try (InputStream is = storage.get(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + namespaceFileUri))) {
            assertThat(new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.joining("\n")), is(expectedFileContent));
        }
    }
}
