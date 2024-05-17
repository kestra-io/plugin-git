package io.kestra.plugin.git;

import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.YamlFlowParser;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.KestraIgnore;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@MicronautTest
class SyncTest {
    public static final String BRANCH = "reconcile";
    public static final String NAMESPACE = "my.namespace";
    public static final String TENANT_ID = "my-tenant";
    public static final Pattern NAMESPACE_FINDER_PATTERN = Pattern.compile("(?m)^namespace: (.*)$");

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FlowRepositoryInterface flowRepositoryInterface;

    @Inject
    private YamlFlowParser yamlFlowParser;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Value("${kestra.git.pat}")
    private String pat;

    @BeforeEach
    void init() throws IOException {
        flowRepositoryInterface.findAllForAllTenants().forEach(flow -> flowRepositoryInterface.delete(flow));
        storageInterface.deleteByPrefix(null, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE)));
        storageInterface.deleteByPrefix(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE)));
    }

    @Test
    void reconcileNsFilesAndFlows() throws Exception {
        // region GIVEN
        // region flows
        // this flow is on Git and should be updated
        String flowSource = """
            id: first-flow
            namespace:\s""" + NAMESPACE + """

            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        Flow flow = yamlFlowParser.parse(flowSource, Flow.class).toBuilder().tenantId(TENANT_ID).build();
        flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        );

        // this flow is not on Git and should be deleted
        Flow flowToDelete = flow.toBuilder().id("flow-to-delete").build();
        flowRepositoryInterface.create(
            flowToDelete,
            flowSource.replace("first-flow", "flow-to-delete"),
            flowToDelete
        );

        // simulate self flow, should not be deleted as it's the flow id of the simulated execution (prevent self deletion)
        String selfFlowId = "self-flow";
        Flow selfFlow = flow.toBuilder().id(selfFlowId).build();
        String selfFlowSource = flowSource.replace("first-flow", selfFlowId);
        flowRepositoryInterface.create(
            selfFlow,
            selfFlowSource,
            selfFlow
        );

        List<Flow> flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(3));
        // endregion

        // region namespace files
        // not in `namespaceFilesDirectory` so it should stay as-is
        String readmeContent = "README content";
        storageInterface.put(
            TENANT_ID,
            URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md"),
            new ByteArrayInputStream(readmeContent.getBytes())
        );
        // will be deleted as it's not on git
        String deletedFilePath = "/sync_directory/file_to_delete.txt";
        storageInterface.put(
            TENANT_ID,
            URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath),
            new ByteArrayInputStream(new byte[0])
        );
        String deletedDirPath = "/sync_directory/dir_to_delete";
        storageInterface.createDirectory(
            TENANT_ID,
            URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)
        );
        String deletedDirSubFilePath = "/sync_directory/dir_to_delete/file_to_delete.txt";
        storageInterface.put(
            TENANT_ID,
            URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath),
            new ByteArrayInputStream(new byte[0])
        );
        // will get updated
        String clonedFilePath = "/sync_directory/cloned.json";
        storageInterface.put(
            TENANT_ID,
            URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + clonedFilePath),
            new ByteArrayInputStream("{\"old-field\": \"old-value\"}".getBytes())
        );

        // check behaviour in case of converting a file to dir or dir to file
        String fileToDirPath = "/sync_directory/file_to_dir";
        storageInterface.put(
            TENANT_ID,
            URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + fileToDirPath),
            new ByteArrayInputStream("some content".getBytes())
        );

        String dirToFilePath = "/sync_directory/dir_to_file";
        storageInterface.put(
            TENANT_ID,
            URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + dirToFilePath + "/file.txt"),
            new ByteArrayInputStream("nested file content".getBytes())
        );
        // endregion
        // endregion

        // region WHEN
        String clonedGitDirectory = "to_clone";
        String destinationDirectory = "sync_directory";
        Sync task = Sync.builder()
            .url("https://github.com/kestra-io/unit-tests")
            .username(pat)
            .password(pat)
            .branch(BRANCH)
            .gitDirectory(clonedGitDirectory)
            .namespaceFilesDirectory(destinationDirectory)
            .build();
        task.run(runContextFactory.of(Map.of("flow", Map.of(
            "namespace", NAMESPACE,
            "id", selfFlowId,
            "tenantId", TENANT_ID
        ))));
        // endregion

        // region THEN
        // region flows
        flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(5));

        RunContext runContext = runContextFactory.of();
        Clone.builder()
            .url("https://github.com/kestra-io/unit-tests")
            .username(pat)
            .password(pat)
            .branch(BRANCH)
            .build()
            .run(runContext);
        assertFlows(TENANT_ID, runContext.tempDir().resolve(Path.of(clonedGitDirectory, "_flows")).toFile(), selfFlowSource);
        // endregion

        // region namespace files
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/" + destinationDirectory + "/" + KestraIgnore.KESTRA_IGNORE_FILE_NAME)), is(false));
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/" + destinationDirectory + "/file_to_ignore.txt")), is(false));
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/" + destinationDirectory + "/dir_to_ignore/file.txt")), is(false));
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/" + destinationDirectory + "/dir_to_ignore")), is(false));
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/" + destinationDirectory + "/_flows")), is(false));
        assertNamespaceFileContent(TENANT_ID, "/README.md", readmeContent);
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedFilePath)), is(false));
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirPath)), is(false));
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + deletedDirSubFilePath)), is(false));
        assertNamespaceFileContent(TENANT_ID, clonedFilePath, "{\"my-field\": \"my-value\"}");
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + fileToDirPath)), is(true));
        assertNamespaceFileContent(TENANT_ID, fileToDirPath + "/file.txt", "directory replacing file");
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + dirToFilePath + "/file.txt")), is(false));
        assertNamespaceFileContent(TENANT_ID, dirToFilePath, "file replacing a directory");
        // endregion
        // endregion
    }

    @Test
    void reconcile_MinimumSetup() throws Exception {
        // region GIVEN
        // region flows
        // this flow is on Git and should be updated


        // this flow is not on Git and should be deleted
        String flowSource = """
            id: flow-to-delete
            namespace:\s""" + NAMESPACE + """
                        
            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        Flow flowToDelete = yamlFlowParser.parse(flowSource, Flow.class).toBuilder().tenantId(TENANT_ID).build();
        flowRepositoryInterface.create(
            flowToDelete,
            flowSource,
            flowToDelete
        );

        // simulate self flow, should not be deleted as it's the flow id of the simulated execution (prevent self deletion)
        String selfFlowId = "self-flow";
        Flow selfFlow = flowToDelete.toBuilder().id(selfFlowId).build();
        String selfFlowSource = flowSource.replace("flow-to-delete", selfFlowId);
        flowRepositoryInterface.create(
            selfFlow,
            selfFlowSource,
            selfFlow
        );

        List<Flow> flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(2));
        // endregion

        // region namespace files
        // not in git so it should be deleted
        String toDeleteFilePath = "/to_delete.txt";
        storageInterface.put(
            TENANT_ID,
            URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + toDeleteFilePath),
            new ByteArrayInputStream("File to delete".getBytes())
        );
        // in git but with another content, should be updated
        storageInterface.put(
            TENANT_ID,
            URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + "/README.md"),
            new ByteArrayInputStream("README content".getBytes())
        );

        // region WHEN
        Sync task = Sync.builder()
            .url("https://github.com/kestra-io/unit-tests")
            .username(pat)
            .password(pat)
            .branch(BRANCH)
            .build();
        task.run(runContextFactory.of(Map.of("flow", Map.of(
            "namespace", NAMESPACE,
            "id", selfFlowId,
            "tenantId", TENANT_ID
        ))));
        // endregion

        // region THEN
        // region flows
        flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(2));

        RunContext runContext = runContextFactory.of();
        Clone.builder()
            .url("https://github.com/kestra-io/unit-tests")
            .username(pat)
            .password(pat)
            .branch(BRANCH)
            .build()
            .run(runContext);
        assertFlows(TENANT_ID, runContext.tempDir().resolve("_flows").toFile(), selfFlowSource);
        // endregion

        // region namespace files
        assertThat(storageInterface.exists(TENANT_ID, URI.create(StorageContext.namespaceFilePrefix(NAMESPACE) + toDeleteFilePath)), is(false));
        assertNamespaceFileContent(TENANT_ID, "/README.md", "This repository is used for unit testing Git integration");
        assertNamespaceFileContent(TENANT_ID, "/ignored.json", "{\"ignored\": true}");
        // endregion
        // endregion
    }


    @Test
    void reconcile_DryRun_ShouldDoNothing() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        logQueue.receive(l -> logs.add(l.getLeft()));
        String namespace = SyncTest.class.getName().toLowerCase();

        String flowSource = """
            id: some-flow
            namespace:\s""" + namespace + """
                        
            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        Flow flow = yamlFlowParser.parse(flowSource, Flow.class);
        flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        );

        flow = flow.toBuilder().id("first-flow").build();
        flowRepositoryInterface.create(
            flow,
            flowSource.replace("some-flow", "first-flow"),
            flow
        );

        flow = flow.toBuilder().id("sub-namespace-flow").namespace(SyncTest.class.getName().toLowerCase() + ".sub").build();
        flowRepositoryInterface.create(
            flow,
            flowSource.replace("some-flow", "sub-namespace-flow"),
            flow
        );

        List<Flow> flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(3));

        String toUpdateFilePath = "/dir_to_file";
        String keptContent = "kept content since dry run";
        storageInterface.put(
            null,
            URI.create(StorageContext.namespaceFilePrefix(namespace) + toUpdateFilePath),
            new ByteArrayInputStream(keptContent.getBytes())
        );

        String someFilePath = "/file.txt";
        String someFileContent = "hello";
        storageInterface.put(
            null,
            URI.create(StorageContext.namespaceFilePrefix(namespace) + someFilePath),
            new ByteArrayInputStream(someFileContent.getBytes())
        );

        Sync task = Sync.builder()
            .id("reconcile")
            .type(Sync.class.getName())
            .url("https://github.com/kestra-io/unit-tests")
            .username(pat)
            .password(pat)
            .branch(BRANCH)
            .gitDirectory("to_clone")
            .dryRun(true)
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Collections.emptyMap());
        task.run(runContext);

        assertThat(flowRepositoryInterface.findAllForAllTenants(), hasSize(3));

        assertNamespaceFileContent(null, namespace, toUpdateFilePath, keptContent);
        assertNamespaceFileContent(null, namespace, someFilePath, someFileContent);
        assertThat(storageInterface.exists(null, URI.create(StorageContext.namespaceFilePrefix(namespace) + "/cloned.json")), is(false));

        assertHasInfoLog(logs, "Dry run is enabled, not performing following actions (- for deletions, + for creations, ~ for update or no modification):");
        assertHasInfoLog(logs, "~ /_flows/first-flow.yml");
        assertHasInfoLog(logs, "~ /_flows/sub-namespace-flow.yml");
        assertHasInfoLog(logs, "- /_flows/some-flow.yml");
        assertHasInfoLog(logs, "+ /_flows/second-flow.yml");
        assertHasInfoLog(logs, "+ /_flows/fakesub-namespace-flow.yml");
        assertHasInfoLog(logs, "~ " + toUpdateFilePath);
        assertHasInfoLog(logs, "- " + someFilePath);
        assertHasInfoLog(logs, "+ /cloned.json");
    }

    private static void assertHasInfoLog(List<LogEntry> logs, String expectedMessage) {
        List<LogEntry> logEntries = TestsUtils.awaitLogs(
            logs,
            logEntry -> logEntry.getLevel().equals(Level.INFO) &&
                logEntry.getMessage().equals(expectedMessage),
            1
        );

        assertThat(logEntries, hasSize(1));
    }

    private void assertFlows(String tenantId, File flowsDir, String selfFlowSource) throws IOException {
        Map<String, String> namespaceForExpectedFlowSources = Stream.concat(
                FileUtils.listFiles(flowsDir, null, true).stream()
                    .filter(file -> !file.getName().equals("kestra-ignored-flow.yml"))
                    .map(throwFunction(file -> FileUtils.readFileToString(file, "UTF-8")))
                    .map(source -> {
                        Matcher matcher = NAMESPACE_FINDER_PATTERN.matcher(source);
                        matcher.find();
                        String previousNamespace = matcher.group(1);
                        if (previousNamespace.startsWith(NAMESPACE + ".")) {
                            return Map.entry(source, previousNamespace);
                        }

                        return Map.entry(matcher.replaceFirst("namespace: " + NAMESPACE), NAMESPACE);
                    }),
                Stream.of(Map.entry(selfFlowSource, NAMESPACE))
            )
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
            ));
        Map<String, String> namespaceForActualFlowSources = flowRepositoryInterface.findWithSource(null, tenantId, NAMESPACE, null).stream()
            .map(flowWithSource -> Map.entry(flowWithSource.getSource(), flowWithSource.getNamespace()))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
            ));
        assertThat(namespaceForActualFlowSources, is(namespaceForExpectedFlowSources));
    }

    private void assertNamespaceFileContent(String tenantId, String namespaceFileUri, String expectedFileContent) throws IOException {
        assertNamespaceFileContent(tenantId, NAMESPACE, namespaceFileUri, expectedFileContent);
    }

    private void assertNamespaceFileContent(String tenantId, String namespace, String namespaceFileUri, String expectedFileContent) throws IOException {
        try (InputStream is = storageInterface.get(tenantId, URI.create(StorageContext.namespaceFilePrefix(namespace) + namespaceFileUri))) {
            assertThat(new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.joining("\n")), is(expectedFileContent));
        }
    }
}
