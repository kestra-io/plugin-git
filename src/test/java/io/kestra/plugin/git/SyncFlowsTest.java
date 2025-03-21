package io.kestra.plugin.git;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.Rethrow;
import io.kestra.core.junit.annotations.KestraTest;
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

@KestraTest
public class SyncFlowsTest extends AbstractGitTest {
    public static final String BRANCH = "sync";
    public static final String GIT_DIRECTORY = "to_clone/_flows";
    public static final String TENANT_ID = "my-tenant";
    public static final String NAMESPACE = "my.namespace";
    public static final String FLOW_ID = "self_flow";
    public static final Pattern NAMESPACE_FINDER_PATTERN = Pattern.compile("(?m)^namespace: (.*)$");

    private static final Map<String, Integer> previousRevisionByUid = new HashMap<>();

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private YamlParser yamlFlowParser;

    @Inject
    private FlowRepositoryInterface flowRepositoryInterface;

    @BeforeEach
    void init() {
        flowRepositoryInterface.findAllForAllTenants().forEach(f -> {
            Flow deleted = flowRepositoryInterface.delete(f.withSource(""));
            previousRevisionByUid.put(deleted.uidWithoutRevision(), deleted.getRevision());
        });
    }

    @Test
    void defaultCase_WithDelete() throws Exception {
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

        RunContext runContext = runContext();

        String invalidFlowSource = """
            id: first-flow
            namespace:\s""" + NAMESPACE + """

            tasks:
              - id: old-task
                type: io.kestra.core.tasks.log.Log
                message: Hello from old-task""";
        Flow invalidFlow = Flow.builder()
            .id("validation-failed-flow")
            .namespace(NAMESPACE)
            .tenantId(TENANT_ID)
            .revision(1)
            .tasks(List.of(new Task() {
                protected String id = "validation-failure";

                protected String type = "unknown.type";

                public String getId(){
                    return this.id;
                }

                public String getType(){
                    return this.type;
                }
            }))
            .build();
        flowRepositoryInterface.create(
            invalidFlow,
            invalidFlowSource,
            invalidFlow
        );

        List<Flow> flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(6));
        flows.forEach(f -> previousRevisionByUid.put(f.uidWithoutRevision(), f.getRevision()));

        SyncFlows task = SyncFlows.builder()
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .gitDirectory(new Property<>("{{gitDirectory}}"))
            .targetNamespace(new Property<>("{{namespace}}"))
            .delete(Property.of(true))
            .includeChildNamespaces(Property.of(true))
            .ignoreInvalidFlows(Property.of(true))
            .build();
        SyncFlows.Output syncOutput = task.run(runContext);

        flowRepositoryInterface.delete(invalidFlow.withSource(""));

        flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(6));

        RunContext cloneRunContext = runContextFactory.of();
        Clone.builder()
            .url(new Property<>(repositoryUrl))
            .username(new Property<>(pat))
            .password(new Property<>(pat))
            .branch(new Property<>(BRANCH))
            .build()
            .run(cloneRunContext);
        assertFlows(cloneRunContext.workingDir().path().resolve(Path.of(GIT_DIRECTORY)).toFile(), true, selfFlowSource);

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(true, new HashMap<>(Map.of("syncState", "DELETED", "flowId", "flow-to-delete", "namespace", "my.namespace.child", "revision", previousRevisionByUid.getOrDefault(Flow.uidWithoutRevision(TENANT_ID, flowToDelete.getNamespace(), flowToDelete.getId()), 1))) {{
            this.put("gitPath", null);
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
        flows.forEach(f -> previousRevisionByUid.put(f.uidWithoutRevision(), f.getRevision()));

        SyncFlows task = SyncFlows.builder()
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .gitDirectory(new Property<>("{{gitDirectory}}"))
            .targetNamespace(new Property<>("{{namespace}}"))
            .includeChildNamespaces(Property.of(true))
            .build();
        SyncFlows.Output syncOutput = task.run(runContext);

        flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(7));

        RunContext cloneRunContext = runContextFactory.of();
        Clone.builder()
            .url(new Property<>(repositoryUrl))
            .username(new Property<>(pat))
            .password(new Property<>(pat))
            .branch(new Property<>(BRANCH))
            .build()
            .run(cloneRunContext);
        assertFlows(cloneRunContext.workingDir().path().resolve(Path.of(GIT_DIRECTORY)).toFile(), true, selfFlowSource, nonVersionedFlowSource);

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(true));
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
        flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        );

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
        assertThat(flows, hasSize(6));
        flows.forEach(f -> previousRevisionByUid.put(f.uidWithoutRevision(), f.getRevision()));

        SyncFlows task = SyncFlows.builder()
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .gitDirectory(new Property<>("{{gitDirectory}}"))
            .targetNamespace(new Property<>("{{namespace}}"))
            .delete(Property.of(true))
            .includeChildNamespaces(Property.of(false))
            .build();
        SyncFlows.Output syncOutput = task.run(runContext);

        flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(6));

        RunContext cloneRunContext = runContextFactory.of();
        Clone.builder()
            .url(new Property<>(repositoryUrl))
            .username(new Property<>(pat))
            .password(new Property<>(pat))
            .branch(new Property<>(BRANCH))
            .build()
            .run(cloneRunContext);
        assertFlows(cloneRunContext.workingDir().path().resolve(Path.of(GIT_DIRECTORY)).toFile(), false, selfFlowSource, unversionedFlowSourceInChildNamespace);

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(false, new HashMap<>(Map.of("syncState", "DELETED", "flowId", "flow-to-delete", "namespace", "my.namespace", "revision", previousRevisionByUid.getOrDefault(Flow.uidWithoutRevision(TENANT_ID, flowToDelete.getNamespace(), flowToDelete.getId()), 1))) {{
            this.put("gitPath", null);
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
        flowRepositoryInterface.create(
            flow,
            flowSource,
            flow
        );

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
        flows.forEach(f -> previousRevisionByUid.put(f.uidWithoutRevision(), f.getRevision()));

        String[] beforeUpdateSources = flowRepositoryInterface.findWithSource(null, TENANT_ID, null, null, null).stream()
            .map(FlowWithSource::getSource)
            .toArray(String[]::new);

        SyncFlows task = SyncFlows.builder()
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .gitDirectory(new Property<>("{{gitDirectory}}"))
            .targetNamespace(new Property<>("{{namespace}}"))
            .delete(Property.of(true))
            .includeChildNamespaces(Property.of(true))
            .dryRun(Property.of(true))
            .build();
        SyncFlows.Output syncOutput = task.run(runContext);

        flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(5));

        String[] afterUpdateSources = flowRepositoryInterface.findWithSource(null, TENANT_ID, null, null, null).stream()
            .map(FlowWithSource::getSource)
            .toArray(String[]::new);

        assertThat(afterUpdateSources, arrayContainingInAnyOrder(beforeUpdateSources));

        assertDiffs(runContext, syncOutput.diffFileUri(), defaultCaseDiffs(true, new HashMap<>(Map.of("syncState", "DELETED", "flowId", "flow-to-delete", "namespace", "my.namespace.child", "revision", previousRevisionByUid.getOrDefault(Flow.uidWithoutRevision(TENANT_ID, flowToDelete.getNamespace(), flowToDelete.getId()), 1))) {{
            this.put("gitPath", null);
        }}));
    }

    private List<Map<String, Object>> defaultCaseDiffs(boolean includeSubNamespaces, Map<String, Object>... additionalDiffs) {
        List<Map<String, Object>> diffs = new ArrayList<>(List.of(
            Map.of("gitPath", "to_clone/_flows/unchanged-flow.yaml", "syncState", "UNCHANGED", "flowId", "unchanged-flow", "namespace", NAMESPACE, "revision", previousRevisionByUid.getOrDefault(Flow.uidWithoutRevision(TENANT_ID, NAMESPACE, "unchanged-flow"), 1)),
            Map.of("gitPath", "to_clone/_flows/first-flow.yml", "syncState", "UPDATED", "flowId", "first-flow", "namespace", NAMESPACE, "revision", previousRevisionByUid.getOrDefault(Flow.uidWithoutRevision(TENANT_ID, NAMESPACE, "first-flow"), 0) + 1),
            Map.of("gitPath", "to_clone/_flows/second-flow.yml", "syncState", "ADDED", "flowId", "second-flow", "namespace", NAMESPACE, "revision", previousRevisionByUid.getOrDefault(Flow.uidWithoutRevision(TENANT_ID, NAMESPACE, "second-flow"), 0) + 1)
        ));

        if (includeSubNamespaces) {
            diffs.add(Map.of("gitPath", "to_clone/_flows/nested/namespace/nested_flow.yaml", "syncState", "ADDED", "flowId", "nested-flow", "namespace", "my.namespace.nested.namespace", "revision", previousRevisionByUid.getOrDefault(Flow.uidWithoutRevision(TENANT_ID, "my.namespace.nested.namespace", "nested-flow"), 0) + 1));
        }

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
            "url", repositoryUrl,
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

    private void assertFlows(File flowsDir, boolean includeSubNamespaces, String... additionalFlowSources) throws IOException {
        Path flowsPath = flowsDir.toPath();
        try (Stream<Path> flows = Files.walk(flowsPath, includeSubNamespaces ? Integer.MAX_VALUE : 1)) {
            String[] expectedFlowSources = Stream.concat(
                flows
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        String fileName = path.getFileName().toString();
                        return !fileName.equals("kestra-ignored-flow.yml") && !fileName.equals(".kestraignore") && !path.toString().contains(".git");
                    })
                    .map(throwFunction(path -> {
                        String rawSource = Files.readString(path);
                        Matcher nsMatcher = NAMESPACE_FINDER_PATTERN.matcher(rawSource);
                        nsMatcher.find();
                        Path parent = flowsPath.relativize(path).getParent();

                        return nsMatcher.replaceFirst("namespace: " + NAMESPACE + (parent == null ? "" : "." + parent.toString().replace("/", "."))).stripTrailing();
                    })),
                Arrays.stream(additionalFlowSources)
            ).toArray(String[]::new);
            String[] actualFlowSources = flowRepositoryInterface.findWithSource(null, SyncFlowsTest.TENANT_ID, null, NAMESPACE, null).stream()
                .map(FlowWithSource::getSource)
                .toArray(String[]::new);
            assertThat(actualFlowSources, arrayContainingInAnyOrder(expectedFlowSources));
        }
    }
}
