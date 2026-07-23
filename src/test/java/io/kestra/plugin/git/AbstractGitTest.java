package io.kestra.plugin.git;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.repositories.FlowRepositoryInterface;

import io.micronaut.context.annotation.Value;

@KestraTest
public abstract class AbstractGitTest {

    /**
     * Persists a flow whose latest revision is a draft, used to assert that Git tasks skip drafts.
     */
    protected static FlowWithSource createDraftFlow(FlowRepositoryInterface flowRepository, String tenantId, String flowId, String namespace) {
        String flowSource = """
            id: %s
            namespace: %s

            draft: true
            tasks:
              - id: my-task
                type: io.kestra.plugin.core.log.Log
                message: Hello from my-task
            """.formatted(flowId, namespace);

        return flowRepository.create(GenericFlow.fromYaml(tenantId, flowSource));
    }

    @Value("${kestra.git.pat}")
    protected String pat;

    @Value("${kestra.git.repository-url}")
    protected String repositoryUrl;

    @Value("${kestra.git.user.email}")
    protected String gitUserEmail;

    @Value("${kestra.git.user.name}")
    protected String gitUserName;

    @Value("${kestra.gitea.pat}")
    protected String giteaPat;

    @Value("${kestra.gitea.repository-url}")
    protected String giteaRepoUrl;

    @Value("${kestra.gitea.user.name}")
    protected String giteaUserName;

    @Value("${kestra.gitea.ca-pem-path:}")
    protected String giteaCaPemPath;
}
