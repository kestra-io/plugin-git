package io.kestra.plugin.git;

import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class SyncFlowTest extends AbstractGitTest {
    public static final String BRANCH = "sync";
    public static final String TENANT_ID = TenantService.MAIN_TENANT;
    public static final String TARGET_NAMESPACE = "io.kestra.synced";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FlowRepositoryInterface flowRepositoryInterface;

    @BeforeEach
    void init() {
        flowRepositoryInterface.findAllForAllTenants().forEach(f -> {
            flowRepositoryInterface.delete(FlowWithSource.of(f, ""));
        });
    }

    @Test
    void createNewFlow() throws Exception {
        RunContext runContext = runContext();

        assertThat(flowRepositoryInterface.findAllForAllTenants().size(), is(0));

        SyncFlow task = SyncFlow.builder()
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .targetNamespace(new Property<>(TARGET_NAMESPACE))
            .flowPath(new Property<>("to_clone/_flows/first-flow.yml"))
            .build();

        SyncFlow.Output output = task.run(runContext);

        assertThat(output.getFlowId(), is("first-flow"));
        assertThat(output.getNamespace(), is(TARGET_NAMESPACE));
        assertThat(output.getRevision(), is(1));

        List<Flow> flows = flowRepositoryInterface.findAllForAllTenants();
        assertThat(flows, hasSize(1));

        Flow syncedFlow = flows.getFirst();
        assertThat(syncedFlow.getId(), is("first-flow"));
        assertThat(syncedFlow.getNamespace(), is(TARGET_NAMESPACE));
        assertThat(syncedFlow.getRevision(), is(1));
    }

    @Test
    void updateExistingFlow() throws Exception {
        RunContext runContext = runContext();

        String initialSource = "id: second-flow\nnamespace: " + TARGET_NAMESPACE + "\ntasks:\n  - id: log\n    type: io.kestra.core.tasks.log.Log\n    message: 'v1'";
        GenericFlow flow = GenericFlow.fromYaml(TENANT_ID, initialSource);

        flowRepositoryInterface.create(
            flow.toBuilder().source(initialSource).build()
        );

        Flow initialFlow = flowRepositoryInterface.findAllForAllTenants().get(0);
        assertThat(initialFlow.getRevision(), is(1));

        SyncFlow task = SyncFlow.builder()
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .targetNamespace(new Property<>(TARGET_NAMESPACE))
            .flowPath(new Property<>("to_clone/_flows/second-flow.yml"))
            .build();

        SyncFlow.Output output = task.run(runContext);

        assertThat(output.getRevision(), is(2));

        List<FlowWithSource> flowsWithSource = flowRepositoryInterface.findByNamespacePrefixWithSource(TENANT_ID, TARGET_NAMESPACE);
        assertThat(flowsWithSource, hasSize(1));

        FlowWithSource updatedFlowWithSource = flowsWithSource.getFirst();

        assertThat(updatedFlowWithSource.getId(), is("second-flow"));
        assertThat(updatedFlowWithSource.getRevision(), is(2));
        assertThat(updatedFlowWithSource.getSource(), containsString("Hello from second-flow"));
    }

    @Test
    void dryRun() throws Exception {
        RunContext runContext = runContext();

        assertThat(flowRepositoryInterface.findAllForAllTenants().size(), is(0));

        SyncFlow task = SyncFlow.builder()
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .targetNamespace(new Property<>(TARGET_NAMESPACE))
            .flowPath(new Property<>("to_clone/_flows/first-flow.yml"))
            .dryRun(Property.of(true))
            .build();

        SyncFlow.Output output = task.run(runContext);

        assertThat(output.getFlowId(), is("first-flow"));
        assertThat(output.getNamespace(), is(TARGET_NAMESPACE));
        assertThat(output.getRevision(), is(1));

        assertThat(flowRepositoryInterface.findAllForAllTenants().size(), is(0));
    }

    @Test
    void fileNotFound() {
        RunContext runContext = runContext();

        SyncFlow task = SyncFlow.builder()
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .targetNamespace(new Property<>(TARGET_NAMESPACE))
            .flowPath(new Property<>("non_existent_file.yml"))
            .build();

        Exception exception = org.junit.jupiter.api.Assertions.assertThrows(
            java.io.FileNotFoundException.class,
            () -> task.run(runContext)
        );
        assertThat(exception.getMessage(), containsString("non_existent_file.yml"));
    }

    private RunContext runContext() {
        return runContextFactory.of(Map.of(
            "flow", Map.of(
                "tenantId", TENANT_ID,
                "namespace", "io.kestra.unittest",
                "id", "test-flow"
            ),
            "url", repositoryUrl,
            "pat", pat,
            "branch", BRANCH
        ));
    }
}