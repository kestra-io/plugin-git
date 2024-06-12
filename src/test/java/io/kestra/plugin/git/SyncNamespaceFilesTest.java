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
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class SyncNamespaceFilesTest {
    public static final String BRANCH = "sync";
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
        assertThat(illegalArgumentException.getMessage(), is("It looks like you have hard-coded Git credentials. Make sure to pass the credential securely using a Pebble expression (e.g. using secrets or environment variables)."));
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

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(true));
    }

    private static List<Map<String, String>> defaultCaseDiffs(boolean withDeleted) {
        ArrayList<Map<String, String>> diffs = new ArrayList<>(List.of(
                Map.of("gitPath", "to_clone/_flows/", "syncState", "ADDED", "kestraPath", "/_flows/"),
                Map.of("gitPath", "to_clone/_flows/nested/", "syncState", "ADDED", "kestraPath", "/_flows/nested/"),
                Map.of("gitPath", "to_clone/_flows/nested/namespace/", "syncState", "ADDED", "kestraPath", "/_flows/nested/namespace/"),
                Map.of("gitPath", "to_clone/_flows/nested/namespace/nested_flow.yaml", "syncState", "ADDED", "kestraPath", "/_flows/nested/namespace/nested_flow.yaml"),
                Map.of("gitPath", "to_clone/_flows/first-flow.yml", "syncState", "ADDED", "kestraPath", "/_flows/first-flow.yml"),
                Map.of("gitPath", "to_clone/_flows/unchanged-flow.yaml", "syncState", "ADDED", "kestraPath", "/_flows/unchanged-flow.yaml"),
                Map.of("gitPath", "to_clone/_flows/.kestraignore", "syncState", "ADDED", "kestraPath", "/_flows/.kestraignore"),
                Map.of("gitPath", "to_clone/_flows/kestra-ignored-flow.yml", "syncState", "ADDED", "kestraPath", "/_flows/kestra-ignored-flow.yml"),
                Map.of("gitPath", "to_clone/_flows/second-flow.yml", "syncState", "ADDED", "kestraPath", "/_flows/second-flow.yml"),
                Map.of("gitPath", "to_clone/cloned.json", "syncState", "OVERWRITTEN", "kestraPath", "/cloned.json")
        ));

        if (withDeleted) {
            diffs.addAll(List.of(
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/file_to_delete.txt"));
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
