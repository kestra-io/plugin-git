package io.kestra.plugin.git;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PushFlowsRewriteNamespaceTest {

    @Test
    void onlyChangesTopLevelNamespaceKey() {
        var source = """
            id: source_flow
            namespace: bankdev
            tasks:
              - id: call
                type: io.kestra.plugin.core.flow.Subflow
                namespace: bankdevops.tools
                flowId: helper
              - id: note
                type: io.kestra.plugin.core.log.Log
                message: |
                  example config for a colleague:
                  namespace: bankdev
                  end
            """;

        var expected = source.replace("namespace: bankdev\ntasks", "namespace: bankprod\ntasks");

        assertThat(PushFlows.namespaceRewriter("bankdev", "bankprod").apply(source), is(expected));
    }

    @Test
    void rewritesChildNamespaceAndTreatsNamespacesAsLiterals() {
        assertThat(
            PushFlows.namespaceRewriter("company.team", "prod.$1").apply("id: a\nnamespace: company.team.child\n"),
            is("id: a\nnamespace: prod.$1.child\n")
        );
        assertThat(
            PushFlows.namespaceRewriter("company.team", "prod").apply("id: a\nnamespace: companyXteam\n"),
            is("id: a\nnamespace: companyXteam\n")
        );
    }

    @Test
    void keepsTrailingCommentAndSkipsLookalikeNamespaces() {
        assertThat(
            PushFlows.namespaceRewriter("bankdev", "bankprod").apply("id: a\nnamespace: bankdev # x\n"),
            is("id: a\nnamespace: bankprod # x\n")
        );
        assertThat(
            PushFlows.namespaceRewriter("bankdev", "bankprod").apply("id: a\nnamespace: bankdev-x\n"),
            is("id: a\nnamespace: bankdev-x\n")
        );
        assertThat(
            PushFlows.namespaceRewriter("bankdev", "bankprod").apply("id: a\nnamespace: bankdev_x\n"),
            is("id: a\nnamespace: bankdev_x\n")
        );
    }
}
