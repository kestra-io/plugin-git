package io.kestra.plugin.git;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

class PushFlowsFilterTest {

    private FlowWithSource createFlow(String id, String namespace, boolean isDraft) {
        Flow flow = Flow.builder()
            .id(id)
            .namespace(namespace)
            .draft(isDraft)
            .build();
        return FlowWithSource.of(flow, "id: " + id + "\nnamespace: " + namespace);
    }

    @Test
    void filterFlowsToPush_whenIncludeChildrenTrue_excludesSiblingNamespacesWithSamePrefix() {
        String sourceNamespace = "dev";

        FlowWithSource exactFlow = createFlow("exact", "dev", false);
        FlowWithSource childFlow = createFlow("child", "dev.child", false);
        FlowWithSource nestedChildFlow = createFlow("nested", "dev.child.nested", false);
        FlowWithSource siblingFlow1 = createFlow("sibling1", "devops", false);
        FlowWithSource siblingFlow2 = createFlow("sibling2", "dev_staging", false);
        FlowWithSource siblingFlow3 = createFlow("sibling3", "developer", false);
        FlowWithSource draftFlow = createFlow("draft", "dev.child", true);

        List<FlowWithSource> allFlows = List.of(
            exactFlow,
            childFlow,
            nestedChildFlow,
            siblingFlow1,
            siblingFlow2,
            siblingFlow3,
            draftFlow
        );

        List<FlowWithSource> filtered = PushFlows.filterFlowsToPush(allFlows, sourceNamespace, true, null);

        assertThat(filtered, containsInAnyOrder(exactFlow, childFlow, nestedChildFlow));
    }

    @Test
    void filterFlowsToPush_whenIncludeChildrenFalse_onlyIncludesExactNamespace() {
        String sourceNamespace = "dev";

        FlowWithSource exactFlow = createFlow("exact", "dev", false);
        FlowWithSource childFlow = createFlow("child", "dev.child", false);
        FlowWithSource siblingFlow = createFlow("sibling", "devops", false);

        List<FlowWithSource> allFlows = List.of(exactFlow, childFlow, siblingFlow);

        List<FlowWithSource> filtered = PushFlows.filterFlowsToPush(allFlows, sourceNamespace, false, null);

        assertThat(filtered, containsInAnyOrder(exactFlow));
    }

    @Test
    void resolveFlowPath_correctlyResolvesPathsForRootAndChildNamespaces() {
        Path flowDir = Path.of("_flows");
        String sourceNamespace = "dev";

        FlowWithSource rootFlow = createFlow("root", "dev", false);
        FlowWithSource childFlow = createFlow("child", "dev.child", false);
        FlowWithSource nestedFlow = createFlow("nested", "dev.child.sub", false);

        assertThat(PushFlows.resolveFlowPath(flowDir, rootFlow, sourceNamespace), is(Path.of("_flows/root.yml")));
        assertThat(PushFlows.resolveFlowPath(flowDir, childFlow, sourceNamespace), is(Path.of("_flows/child/child.yml")));
        assertThat(PushFlows.resolveFlowPath(flowDir, nestedFlow, sourceNamespace), is(Path.of("_flows/child/sub/nested.yml")));
    }
}
