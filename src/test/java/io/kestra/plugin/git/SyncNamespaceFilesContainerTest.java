package io.kestra.plugin.git;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.git.shared.testkit.AbstractKestraContainerTest;
import io.kestra.sdk.KestraClient;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration test for {@link SyncNamespaceFiles} against a live Kestra container.
 * Reproduces <a href="https://github.com/kestra-io/plugin-git/issues/338">#338</a>: syncing into a
 * namespace that doesn't exist yet must create it, so it is listed and its files are visible via the API,
 * not just written to internal storage.
 *
 * <p>
 * The Kestra container is started once per class by {@link AbstractKestraContainerTest}.
 * Kestra API credentials are hardcoded for test use only.
 * GitHub credentials are read from the {@code kestra.git.pat} Micronaut property (set via {@code GH_PERSONAL_TOKEN}).
 */
@KestraTest
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.username", value = "admin@admin.com")
@io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.password", value = "Root!1234")
public class SyncNamespaceFilesContainerTest extends AbstractKestraContainerTest {

    private static final String TARGET_NAMESPACE = "io.kestra.tests.container.syncnamespacefiles";
    private static final String REPO_URL = "https://github.com/kestra-io/unit-tests";
    private static final String BRANCH = "sync";
    private static final String GIT_DIRECTORY = "to_clone";

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.git.pat}")
    private String gitPat;

    @Test
    void syncIntoMissingNamespace_shouldCreateNamespaceAndSyncFiles() throws Exception {
        var runContext = buildRunContext();

        var task = SyncNamespaceFiles.builder()
            .url(Property.ofValue(REPO_URL))
            .username(Property.ofValue(gitPat))
            .password(Property.ofValue(gitPat))
            .branch(Property.ofValue(BRANCH))
            .gitDirectory(Property.ofValue(GIT_DIRECTORY))
            .namespace(Property.ofValue(TARGET_NAMESPACE))
            .kestraUrl(Property.ofValue(kestraUrl))
            .build();

        SyncNamespaceFiles.Output output = task.run(runContext);

        assertThat("output must be present", output, notNullValue());
        assertThat("diff file must be present", output.diffFileUri(), notNullValue());

        var client = KestraClient.builder().url(kestraUrl).basicAuth(USERNAME, PASSWORD).build();

        var namespace = client.namespaces().namespace(TARGET_NAMESPACE, TENANT_ID);
        assertThat("the namespace must have been created and listed via the API", namespace, notNullValue());
        assertThat(namespace.getId(), is(TARGET_NAMESPACE));

        var files = client.files().listNamespaceDirectoryFiles(TARGET_NAMESPACE, TENANT_ID, "/");
        assertThat("synced files must be visible via the API", files, not(empty()));
    }

    private RunContext buildRunContext() {
        var rc = runContextFactory.of(
            Map.of(
                "flow", Map.of(
                    "tenantId", TENANT_ID,
                    "namespace", "system",
                    "id", "sync-namespace-files-container-test"
                )
            )
        );
        runContextFactory.initializer().forExecutor((DefaultRunContext) rc);
        return rc;
    }
}
