package io.kestra.plugin.git;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PushFlowsRewriteNamespaceTest {

    @Test
    void onlyChangesTopLevelNamespaceKey() {
        String source = """
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

        String expected = source.replace("namespace: bankdev\ntasks", "namespace: bankprod\ntasks");

        assertThat(PushFlows.rewriteNamespace(source, "bankdev", "bankprod"), is(expected));
    }

    @Test
    void rewritesChildNamespaceAndTreatsNamespacesAsLiterals() {
        assertThat(
            PushFlows.rewriteNamespace("id: a\nnamespace: company.team.child\n", "company.team", "prod.$1"),
            is("id: a\nnamespace: prod.$1.child\n")
        );
        assertThat(
            PushFlows.rewriteNamespace("id: a\nnamespace: companyXteam\n", "company.team", "prod"),
            is("id: a\nnamespace: companyXteam\n")
        );
    }
}
