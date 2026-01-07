package io.kestra.plugin.git;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.Namespace;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.KestraIgnore;
import io.kestra.core.utils.Rethrow;
import jakarta.inject.Inject;
import java.nio.file.Path;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
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
    private StorageInterface storage;

    @BeforeEach
    void init() throws IOException {
        storage.deleteByPrefix(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE)));
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
        String deletedFilePath = "/file_to_delete.txt";
        runContext.storage().namespace(NAMESPACE).putFile(
            Path.of(deletedFilePath),
            new ByteArrayInputStream(new byte[0])
        );
        String deletedDirPath = "/dir_to_delete";
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
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .gitDirectory(new Property<>("{{gitDirectory}}"))
            .namespace(new Property<>("{{namespace}}"))
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
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(deletedDirPath)), is(false));
        assertThat(runContext.storage().namespace(NAMESPACE).exists(Path.of(deletedDirSubFilePath)), is(false));
        assertNamespaceFileContent(runContext, clonedFilePath, "{\"my-field\": \"my-value\"}");

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(true));
    }

    @Test
    void defaultCase_WithoutDelete() throws Exception {
        RunContext runContext = runContext();

        // not in `gitDirectory` so it should be deleted but since delete flag is false it won't
        storage.put(
                TENANT_ID,
                NAMESPACE,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md"),
                new ByteArrayInputStream("README content".getBytes())
        );
        // will not be deleted as it's not on git but delete flag is false
        String deletedFilePath = "/file_to_delete.txt";
        storage.put(
                TENANT_ID,
                NAMESPACE,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath),
                new ByteArrayInputStream(new byte[0])
        );
        String deletedDirPath = "/dir_to_delete";
        storage.createDirectory(
                TENANT_ID,
                NAMESPACE,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)
        );
        String deletedDirSubFilePath = "/dir_to_delete/file_to_delete.txt";
        storage.put(
                TENANT_ID,
                NAMESPACE,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath),
                new ByteArrayInputStream(new byte[0])
        );
        // will get updated
        String clonedFilePath = "/cloned.json";
        storage.put(
                TENANT_ID,
                NAMESPACE,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + clonedFilePath),
                new ByteArrayInputStream("{\"old-field\": \"old-value\"}".getBytes())
        );

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
                .url(new Property<>("{{url}}"))
                .username(new Property<>("{{pat}}"))
                .password(new Property<>("{{pat}}"))
                .branch(new Property<>("{{branch}}"))
                .gitDirectory(new Property<>("{{gitDirectory}}"))
                .namespace(new Property<>("{{namespace}}"))
                .build();
        SyncNamespaceFiles.Output syncOutput = task.run(runContext);

        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/" + KestraIgnore.KESTRA_IGNORE_FILE_NAME)), is(false));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/file_to_ignore.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/dir_to_ignore/file.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/dir_to_ignore")), is(false));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/_flows/first-flow.yml")), is(true));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md")), is(true));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath)), is(true));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)), is(true));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath)), is(true));
        assertNamespaceFileContent(runContext, clonedFilePath, "{\"my-field\": \"my-value\"}");

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(false));
    }

    @Test
    void defaultCase_DryRunWithDeleteFlag_ShouldStillNotifyWhatWouldBeDeleted() throws Exception {
        RunContext runContext = runContext();

        // not in `gitDirectory` so it should be deleted
        storage.put(
                TENANT_ID,
                NAMESPACE,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md"),
                new ByteArrayInputStream("README content".getBytes())
        );
        // will be deleted as it's not on git
        String deletedFilePath = "/file_to_delete.txt";
        storage.put(
                TENANT_ID,
                NAMESPACE,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath),
                new ByteArrayInputStream(new byte[0])
        );
        String deletedDirPath = "/dir_to_delete";
        storage.createDirectory(
                TENANT_ID,
                NAMESPACE,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)
        );
        String deletedDirSubFilePath = "/dir_to_delete/file_to_delete.txt";
        storage.put(
                TENANT_ID,
                NAMESPACE,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath),
                new ByteArrayInputStream(new byte[0])
        );
        // will get updated
        String clonedFilePath = "/cloned.json";
        storage.put(
                TENANT_ID,
                NAMESPACE,
                URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + clonedFilePath),
                new ByteArrayInputStream("{\"old-field\": \"old-value\"}".getBytes())
        );

        SyncNamespaceFiles task = SyncNamespaceFiles.builder()
                .url(new Property<>("{{url}}"))
                .username(new Property<>("{{pat}}"))
                .password(new Property<>("{{pat}}"))
                .branch(new Property<>("{{branch}}"))
                .gitDirectory(new Property<>("{{gitDirectory}}"))
                .namespace(new Property<>("{{namespace}}"))
                .dryRun(Property.ofValue(true))
                .delete(Property.ofValue(true))
                .build();
        SyncNamespaceFiles.Output syncOutput = task.run(runContext);

        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/" + KestraIgnore.KESTRA_IGNORE_FILE_NAME)), is(false));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/file_to_ignore.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/dir_to_ignore/file.txt")), is(false));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/dir_to_ignore")), is(false));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/_flows/first-flow.yml")), is(false));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md")), is(true));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath)), is(true));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)), is(true));
        assertThat(storage.exists(TENANT_ID, NAMESPACE, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath)), is(true));
        assertNamespaceFileContent(runContext, clonedFilePath, "{\"old-field\": \"old-value\"}");

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(true));
    }

    private static List<Map<String, String>> defaultCaseDiffs(boolean withDeleted) {
        ArrayList<Map<String, String>> diffs = new ArrayList<>(List.of(
                Map.of("gitPath", "to_clone/_flows/", "syncState", "ADDED", "kestraPath", "/kestra/my/namespace/_files/_flows/"),
                Map.of("gitPath", "to_clone/_flows/nested/", "syncState", "ADDED", "kestraPath", "/kestra/my/namespace/_files/_flows/nested/"),
                Map.of("gitPath", "to_clone/_flows/nested/namespace/", "syncState", "ADDED", "kestraPath", "/kestra/my/namespace/_files/_flows/nested/namespace/"),
                Map.of("gitPath", "to_clone/_flows/nested/namespace/nested_flow.yaml", "syncState", "ADDED", "kestraPath", "/kestra/my/namespace/_files/_flows/nested/namespace/nested_flow.yaml"),
                Map.of("gitPath", "to_clone/_flows/first-flow.yml", "syncState", "ADDED", "kestraPath", "/kestra/my/namespace/_files/_flows/first-flow.yml"),
                Map.of("gitPath", "to_clone/_flows/unchanged-flow.yaml", "syncState", "ADDED", "kestraPath", "/kestra/my/namespace/_files/_flows/unchanged-flow.yaml"),
                Map.of("gitPath", "to_clone/_flows/.kestraignore", "syncState", "ADDED", "kestraPath", "/kestra/my/namespace/_files/_flows/.kestraignore"),
                Map.of("gitPath", "to_clone/_flows/kestra-ignored-flow.yml", "syncState", "ADDED", "kestraPath", "/kestra/my/namespace/_files/_flows/kestra-ignored-flow.yml"),
                Map.of("gitPath", "to_clone/_flows/second-flow.yml", "syncState", "ADDED", "kestraPath", "/kestra/my/namespace/_files/_flows/second-flow.yml"),
                Map.of("gitPath", "to_clone/cloned.json", "syncState", "OVERWRITTEN", "kestraPath", "/kestra/my/namespace/_files/cloned.json")
        ));

        if (withDeleted) {
            diffs.addAll(List.of(
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/kestra/my/namespace/_files/file_to_delete.txt"));
                        this.put("gitPath", null);
                    }},
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/kestra/my/namespace/_files/dir_to_delete/file_to_delete.txt"));
                        this.put("gitPath", null);
                    }},
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/kestra/my/namespace/_files/dir_to_delete/"));
                        this.put("gitPath", null);
                    }},
                    new HashMap<>() {{
                        this.putAll(Map.of("syncState", "DELETED", "kestraPath", "/kestra/my/namespace/_files/README.md"));
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
                "url", repositoryUrl,
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

    private void assertNamespaceFileContent(RunContext runContext, String namespaceFileUri, String expectedFileContent) throws IOException {
        Namespace namespace = runContext.storage().namespace(NAMESPACE);
        try (InputStream is = namespace.getFileContent(Path.of(namespaceFileUri))) {
            assertThat(new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.joining("\n")), is(expectedFileContent));
        }
    }
}
