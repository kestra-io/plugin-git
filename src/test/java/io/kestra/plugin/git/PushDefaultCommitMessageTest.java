package io.kestra.plugin.git;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.git.shared.testkit.MockKestraApiServer;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

// Local bare repo, no Git credentials. Namespace comes from the default flow.namespace expression.
@KestraTest
class PushDefaultCommitMessageTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FlowRepositoryInterface flowRepositoryInterface;

    @Test
    void pushNamespaceFiles_unsetCommitMessage_rendersNamespaceInDefault() throws Exception {
        var namespace = IdUtils.create().toLowerCase();
        var branch = IdUtils.create();
        var remote = createLocalBareRepo();

        var runContext = runContext(namespace);
        runContext.storage().namespace(namespace).putFile(Path.of("foo.txt"), new ByteArrayInputStream("foo".getBytes(StandardCharsets.UTF_8)));

        PushNamespaceFiles.builder()
            .id("pushNamespaceFiles")
            .type(PushNamespaceFiles.class.getName())
            .url(Property.ofValue(remote.toUri().toString()))
            .branch(Property.ofValue(branch))
            .authorEmail(Property.ofValue("test@test.com"))
            .authorName(Property.ofValue("test"))
            .build()
            .run(runContext);

        assertThat(lastCommitMessage(remote, branch), is("Add files from %s namespace".formatted(namespace)));
    }

    @Test
    void pushFlows_unsetCommitMessage_rendersNamespaceInDefault() throws Exception {
        var namespace = IdUtils.create().toLowerCase();
        var branch = IdUtils.create();
        var remote = createLocalBareRepo();

        flowRepositoryInterface.create(GenericFlow.fromYaml(TenantService.MAIN_TENANT, """
            id: some-flow
            namespace: %s
            tasks:
              - id: my-task
                type: io.kestra.plugin.core.log.Log
                message: Hello
            """.formatted(namespace)));

        try (MockKestraApiServer server = MockKestraApiServer.start(flowRepositoryInterface)) {
            PushFlows.builder()
                .id("pushFlows")
                .type(PushFlows.class.getName())
                .url(Property.ofValue(remote.toUri().toString()))
                .branch(Property.ofValue(branch))
                .authorEmail(Property.ofValue("test@test.com"))
                .authorName(Property.ofValue("test"))
                .targetNamespace(Property.ofValue(namespace))
                .kestraUrl(Property.ofValue(server.url()))
                .build()
                .run(runContext(namespace));
        }

        assertThat(lastCommitMessage(remote, branch), is("Add files from %s namespace".formatted(namespace)));
    }

    private RunContext runContext(String namespace) {
        return runContextFactory.of(Map.of("flow", Map.of("tenantId", TenantService.MAIN_TENANT, "namespace", namespace)));
    }

    private static String lastCommitMessage(Path remote, String branch) throws Exception {
        var clone = Files.createTempDirectory("unit-test.default-commit-message-clone");
        try (Git git = Git.cloneRepository().setURI(remote.toUri().toString()).setDirectory(clone.toFile()).setBranch(branch).call()) {
            var head = git.log().setMaxCount(1).call().iterator().next();
            return head.getFullMessage();
        } finally {
            FileUtils.deleteQuietly(clone.toFile());
        }
    }

    private static Path createLocalBareRepo() throws IOException, org.eclipse.jgit.api.errors.GitAPIException {
        var repoDir = Files.createTempDirectory("unit-test.default-commit-message-bare");
        Git.init().setDirectory(repoDir.toFile()).setBare(true).call().close();

        var seedClone = Files.createTempDirectory("unit-test.default-commit-message-seed");
        try (Git git = Git.cloneRepository().setURI(repoDir.toUri().toString()).setDirectory(seedClone.toFile()).call()) {
            Files.writeString(seedClone.resolve("README.md"), "seed");
            git.add().addFilepattern(".").call();
            git.commit().setMessage("seed commit").setAuthor("test", "test@test.com").call();
            git.push().call();
        } finally {
            FileUtils.deleteQuietly(seedClone.toFile());
        }
        return repoDir;
    }
}
