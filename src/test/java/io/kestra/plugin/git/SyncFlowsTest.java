package io.kestra.plugin.git;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.serializers.YamlFlowParser;
import io.kestra.core.utils.Rethrow;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
public class SyncFlowsTest {
    public static final String BRANCH = "sync";
    public static final String GIT_DIRECTORY = "to_clone/_flows";
    public static final String TENANT_ID = "my-tenant";
    public static final String NAMESPACE = "my.namespace";
    public static final String URL = "https://github.com/kestra-io/unit-tests";
    public static final String FLOW_ID = "self_flow";
    public static final Pattern NAMESPACE_FINDER_PATTERN = Pattern.compile("(?m)^namespace: (.*)$");

    private final Map<String, Integer> previousRevisionByUid = new HashMap<>();

    @Value("${kestra.git.pat}")
    private String pat;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private YamlFlowParser yamlFlowParser;

    @Inject
    private FlowRepositoryInterface flowRepositoryInterface;

    @Inject
    private QueueInterface<LogEntry> logQueue;

    @BeforeEach
    void init() {
        flowRepositoryInterface.findAllForAllTenants().forEach(flowRepositoryInterface::delete);
    }

    @Test
    void hardcodedPassword() {
        SyncFlows syncFlows = SyncFlows.builder()
            .id("syncFlows")
            .type(PushNamespaceFiles.class.getName())
            .url(URL)
            .password("my-password")
            .build();

        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> syncFlows.run(runContextFactory.of(Map.of(
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

        String flowSource = """
            id: first-flow
            namespace:\s""" + NAMESPACE + """

            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        Flow flow = yamlFlowParser.parse(flowSource, Flow.class).toBuilder().tenantId(TENANT_ID).build();
        previousRevisionByUid.put(flow.uidWithoutRevision(), flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        ).getRevision());

        // this flow is not on Git and should be deleted
        Flow flowToDelete = flow.toBuilder().id("flow-to-delete").namespace(NAMESPACE + ".child").build();
        flowRepositoryInterface.create(
            flowToDelete,
            flowSource.replace("first-flow", flowToDelete.getId()).replace(NAMESPACE, flowToDelete.getNamespace()),
            flowToDelete
        );

        // simulate self flow, should not be deleted as it's the flow id of the simulated execution (prevent self deletion)
        Flow selfFlow = flow.toBuilder().id(FLOW_ID).build();
        String selfFlowSource = flowSource.replace("first-flow", FLOW_ID);
        flowRepositoryInterface.create(
            selfFlow,
            selfFlowSource,
            selfFlow
        );

        // a flow present on git that doesn't have any change
        Flow unchangedFlow = flow.toBuilder().id("unchanged-flow").build();
        previousRevisionByUid.put(unchangedFlow.uidWithoutRevision(), flowRepositoryInterface.create(
            unchangedFlow,
            flowSource.replace("first-flow", unchangedFlow.getId()),
            unchangedFlow
        ).getRevision());

        flowSource = """
            id: unpresent-on-git-flow
            namespace: another.namespace

            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        flow = yamlFlowParser.parse(flowSource, Flow.class).toBuilder().tenantId(TENANT_ID).build();
        flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        );

        List<Flow> flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(5));

        SyncFlows task = SyncFlows.builder()
            .url("{{url}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .branch("{{branch}}")
            .gitDirectory("{{gitDirectory}}")
            .targetNamespace("{{namespace}}")
            .delete(true)
            .includeChildNamespaces(true)
            .build();
        SyncFlows.Output syncOutput = task.run(runContext);

        flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(6));

        runContext = runContextFactory.of();
        Clone.builder()
            .url("https://github.com/kestra-io/unit-tests")
            .username(pat)
            .password(pat)
            .branch(BRANCH)
            .build()
            .run(runContext);
        assertFlows(runContext.tempDir().resolve(Path.of(GIT_DIRECTORY)).toFile(), selfFlowSource);

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(new HashMap<>(Map.of("syncState", "DELETED", "flowId", "flow-to-delete", "namespace", "my.namespace.child")) {{
            this.put("gitPath", null);
            this.put("revision", null);
        }}));
    }

    @Test
    void defaultCase_WithoutDelete() throws Exception {
        RunContext runContext = runContext();

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
        Flow nonVersionedFlow = flow.toBuilder().id("flow-to-delete").build();
        String nonVersionedFlowSource = flowSource.replace("first-flow", nonVersionedFlow.getId());
        flowRepositoryInterface.create(
            nonVersionedFlow,
            nonVersionedFlowSource,
            nonVersionedFlow
        );

        // simulate self flow, should not be deleted as it's the flow id of the simulated execution (prevent self deletion)
        Flow selfFlow = flow.toBuilder().id(FLOW_ID).build();
        String selfFlowSource = flowSource.replace("first-flow", FLOW_ID);
        flowRepositoryInterface.create(
            selfFlow,
            selfFlowSource,
            selfFlow
        );

        // a flow present on git that doesn't have any change
        Flow unchangedFlow = flow.toBuilder().id("unchanged-flow").build();
        flowRepositoryInterface.create(
            unchangedFlow,
            flowSource.replace("first-flow", unchangedFlow.getId()),
            unchangedFlow
        );

        flowSource = """
            id: unpresent-on-git-flow
            namespace: another.namespace

            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        flow = yamlFlowParser.parse(flowSource, Flow.class).toBuilder().tenantId(TENANT_ID).build();
        flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        );

        List<Flow> flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(5));

        SyncFlows task = SyncFlows.builder()
            .url("{{url}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .branch("{{branch}}")
            .gitDirectory("{{gitDirectory}}")
            .targetNamespace("{{namespace}}")
            .includeChildNamespaces(true)
            .build();
        SyncFlows.Output syncOutput = task.run(runContext);

        flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(7));

        runContext = runContextFactory.of();
        Clone.builder()
            .url("https://github.com/kestra-io/unit-tests")
            .username(pat)
            .password(pat)
            .branch(BRANCH)
            .build()
            .run(runContext);
        assertFlows(runContext.tempDir().resolve(Path.of(GIT_DIRECTORY)).toFile(), selfFlowSource, nonVersionedFlowSource);

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs());
    }

    @Test
    void defaultCase_WithDeleteNoChildNs() throws Exception {
        RunContext runContext = runContext();

        String flowSource = """
            id: first-flow
            namespace:\s""" + NAMESPACE + """

            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        Flow flow = yamlFlowParser.parse(flowSource, Flow.class).toBuilder().tenantId(TENANT_ID).build();
        previousRevisionByUid.put(flow.uidWithoutRevision(), flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        ).getRevision());

        // this flow is not on Git and should be deleted
        Flow flowToDelete = flow.toBuilder().id("flow-to-delete").build();
        flowRepositoryInterface.create(
            flowToDelete,
            flowSource.replace("first-flow", flowToDelete.getId()),
            flowToDelete
        );

        // this flow is not on Git but should not be deleted as it's in a child namespace
        Flow unversionedFlowInChildNamespace = flow.toBuilder().id("flow-to-delete").namespace(NAMESPACE + ".child").build();
        String unversionedFlowSourceInChildNamespace = flowSource.replace("first-flow", unversionedFlowInChildNamespace.getId()).replace(NAMESPACE, unversionedFlowInChildNamespace.getNamespace());
        flowRepositoryInterface.create(
            unversionedFlowInChildNamespace,
            unversionedFlowSourceInChildNamespace,
            unversionedFlowInChildNamespace
        );

        // simulate self flow, should not be deleted as it's the flow id of the simulated execution (prevent self deletion)
        Flow selfFlow = flow.toBuilder().id(FLOW_ID).build();
        String selfFlowSource = flowSource.replace("first-flow", FLOW_ID);
        flowRepositoryInterface.create(
            selfFlow,
            selfFlowSource,
            selfFlow
        );

        // a flow present on git that doesn't have any change
        Flow unchangedFlow = flow.toBuilder().id("unchanged-flow").build();
        previousRevisionByUid.put(unchangedFlow.uidWithoutRevision(), flowRepositoryInterface.create(
            unchangedFlow,
            flowSource.replace("first-flow", unchangedFlow.getId()),
            unchangedFlow
        ).getRevision());

        flowSource = """
            id: unpresent-on-git-flow
            namespace: another.namespace

            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        flow = yamlFlowParser.parse(flowSource, Flow.class).toBuilder().tenantId(TENANT_ID).build();
        flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        );

        List<Flow> flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(6));

        SyncFlows task = SyncFlows.builder()
            .url("{{url}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .branch("{{branch}}")
            .gitDirectory("{{gitDirectory}}")
            .targetNamespace("{{namespace}}")
            .delete(true)
            .includeChildNamespaces(false)
            .build();
        SyncFlows.Output syncOutput = task.run(runContext);

        flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(7));

        runContext = runContextFactory.of();
        Clone.builder()
            .url("https://github.com/kestra-io/unit-tests")
            .username(pat)
            .password(pat)
            .branch(BRANCH)
            .build()
            .run(runContext);
        assertFlows(runContext.tempDir().resolve(Path.of(GIT_DIRECTORY)).toFile(), selfFlowSource, unversionedFlowSourceInChildNamespace);

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(new HashMap<>(Map.of("syncState", "DELETED", "flowId", "flow-to-delete", "namespace", "my.namespace")) {{
            this.put("gitPath", null);
            this.put("revision", null);
        }}));
    }

    @Test
    void dryRun_WithDelete() throws Exception {
        RunContext runContext = runContext();

        String flowSource = """
            id: first-flow
            namespace:\s""" + NAMESPACE + """

            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        Flow flow = yamlFlowParser.parse(flowSource, Flow.class).toBuilder().tenantId(TENANT_ID).build();
        previousRevisionByUid.put(flow.uidWithoutRevision(), flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        ).getRevision());

        // this flow is not on Git and should be deleted
        Flow flowToDelete = flow.toBuilder().id("flow-to-delete").namespace(NAMESPACE + ".child").build();
        flowRepositoryInterface.create(
            flowToDelete,
            flowSource.replace("first-flow", flowToDelete.getId()).replace(NAMESPACE, flowToDelete.getNamespace()),
            flowToDelete
        );

        // simulate self flow, should not be deleted as it's the flow id of the simulated execution (prevent self deletion)
        Flow selfFlow = flow.toBuilder().id(FLOW_ID).build();
        String selfFlowSource = flowSource.replace("first-flow", FLOW_ID);
        flowRepositoryInterface.create(
            selfFlow,
            selfFlowSource,
            selfFlow
        );

        // a flow present on git that doesn't have any change
        Flow unchangedFlow = flow.toBuilder().id("unchanged-flow").build();
        previousRevisionByUid.put(unchangedFlow.uidWithoutRevision(), flowRepositoryInterface.create(
            unchangedFlow,
            flowSource.replace("first-flow", unchangedFlow.getId()),
            unchangedFlow
        ).getRevision());

        flowSource = """
            id: unpresent-on-git-flow
            namespace: another.namespace

            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        flow = yamlFlowParser.parse(flowSource, Flow.class).toBuilder().tenantId(TENANT_ID).build();
        flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        );

        List<Flow> flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(5));

        String[] beforeUpdateSources = flowRepositoryInterface.findWithSource(null, TENANT_ID, null, null).stream()
            .map(FlowWithSource::getSource)
            .toArray(String[]::new);

        SyncFlows task = SyncFlows.builder()
            .url("{{url}}")
            .username("{{pat}}")
            .password("{{pat}}")
            .branch("{{branch}}")
            .gitDirectory("{{gitDirectory}}")
            .targetNamespace("{{namespace}}")
            .delete(true)
            .includeChildNamespaces(true)
            .dryRun(true)
            .build();
        SyncFlows.Output syncOutput = task.run(runContext);

        flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(5));

        String[] afterUpdateSources = flowRepositoryInterface.findWithSource(null, TENANT_ID, null, null).stream()
            .map(FlowWithSource::getSource)
            .toArray(String[]::new);

        assertThat(afterUpdateSources, arrayContainingInAnyOrder(beforeUpdateSources));

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(new HashMap<>(Map.of("syncState", "DELETED", "flowId", "flow-to-delete", "namespace", "my.namespace.child")) {{
            this.put("gitPath", null);
            this.put("revision", null);
        }}));
    }

    private List<Map<String, Object>> defaultCaseDiffs(Map<String, Object>... additionalDiffs) {
        List<Map<String, Object>> diffs = new ArrayList<>(List.of(
            // TODO Once revision can be forecasted and we can detect unchanged flows, we should put revision to 1 and maybe add a syncState enum value to UNCHANGED
            Map.of("gitPath", "to_clone/_flows/unchanged-flow.yaml", "syncState", "OVERWRITTEN", "flowId", "unchanged-flow", "namespace", NAMESPACE, "revision", previousRevisionByUid.getOrDefault(Flow.uidWithoutRevision(TENANT_ID, NAMESPACE, "unchanged-flow"), 1) + 1),
            Map.of("gitPath", "to_clone/_flows/first-flow.yml", "syncState", "OVERWRITTEN", "flowId", "first-flow", "namespace", NAMESPACE, "revision", previousRevisionByUid.getOrDefault(Flow.uidWithoutRevision(TENANT_ID, NAMESPACE, "first-flow"), 1) + 1),
            Map.of("gitPath", "to_clone/_flows/nested/namespace/nested_flow.yaml", "syncState", "ADDED", "flowId", "nested-flow", "namespace", "my.namespace.nested.namespace", "revision", 1),
            Map.of("gitPath", "to_clone/_flows/second-flow.yml", "syncState", "ADDED", "flowId", "second-flow", "namespace", NAMESPACE, "revision", 1)
        ));

        diffs.addAll(Arrays.asList(additionalDiffs));
        return diffs;
    }

    private RunContext runContext() {
        return runContextFactory.of(Map.of(
            "flow", Map.of(
                "tenantId", SyncFlowsTest.TENANT_ID,
                "namespace", SyncFlowsTest.NAMESPACE,
                "id", SyncFlowsTest.FLOW_ID
            ),
            "url", SyncFlowsTest.URL,
            "pat", pat,
            "branch", SyncFlowsTest.BRANCH,
            "namespace", SyncFlowsTest.NAMESPACE,
            "gitDirectory", SyncFlowsTest.GIT_DIRECTORY
        ));
    }

    private static void assertDiffs(RunContext runContext, URI diffFileUri, List<Map<String, Object>> expectedDiffs) throws IOException {
        String diffSummary = IOUtils.toString(runContext.storage().getFile(diffFileUri), StandardCharsets.UTF_8);
        List<Map<String, Object>> diffMaps = diffSummary.lines()
            .map(Rethrow.throwFunction(diff -> JacksonMapper.ofIon().readValue(diff, new TypeReference<Map<String, Object>>() {
            })))
            .toList();
        assertThat(diffMaps, containsInAnyOrder(expectedDiffs.toArray(Map[]::new)));
    }

    private void assertFlows(File flowsDir, String... additionalFlowSources) throws IOException {
        Path flowsPath = flowsDir.toPath();
        try (Stream<Path> flows = Files.walk(flowsPath)) {
            String[] expectedFlowSources = Stream.concat(
                flows
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        String fileName = path.getFileName().toString();
                        return !fileName.equals("kestra-ignored-flow.yml") && !fileName.equals(".kestraignore");
                    })
                    .map(throwFunction(path -> {
                        String rawSource = Files.readString(path);
                        Matcher nsMatcher = NAMESPACE_FINDER_PATTERN.matcher(rawSource);
                        nsMatcher.find();
                        Path parent = flowsPath.relativize(path).getParent();

                        return nsMatcher.replaceFirst("namespace: " + NAMESPACE + (parent == null ? "" : "." + parent.toString().replace("/", ".")));
                    })),
                Arrays.stream(additionalFlowSources)
            ).toArray(String[]::new);
            String[] actualFlowSources = flowRepositoryInterface.findWithSource(null, SyncFlowsTest.TENANT_ID, NAMESPACE, null).stream()
                .map(FlowWithSource::getSource)
                .toArray(String[]::new);
            assertThat(actualFlowSources, arrayContainingInAnyOrder(expectedFlowSources));
        }
    }
}
