package io.kestra.plugin.git;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.SDK;
import io.kestra.core.utils.TestsUtils;
import io.kestra.sdk.KestraClient;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The API URL comes from the same default SDK authentication as the credentials, so one instance-wide setting covers both. */
@KestraTest
class KestraApiConnectionTest {
    private static final String DEFAULT_URL = "http://localhost:8080";
    private static final String SDK_DEFAULT_URL = "https://sdk-default.example.com";

    @Inject
    private RunContextFactory runContextFactory;

    /**
     * The default authentication normally comes from the application configuration, but declaring it with
     * {@code @Property} would spawn a second Micronaut context, and the two embedded servers then fight over the same
     * port. Swapping the SDK on the run context keeps the whole suite on a single context.
     */
    private RunContext runContextWithSdkUrl(Task task, String url) throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        SDK sdk = () -> Optional.of(new SDK.Auth(Optional.of(url), Optional.empty(), Optional.empty(), Optional.empty()));
        Field sdkField = runContext.getClass().getDeclaredField("sdk");
        sdkField.setAccessible(true);
        sdkField.set(runContext, sdk);

        return runContext;
    }

    /** Reads the URL the client was built with (the SDK only exposes it on its inner API client). */
    private static String url(KestraClient client) throws Exception {
        Field apiClientField = KestraClient.class.getDeclaredField("apiClient");
        apiClientField.setAccessible(true);
        Object apiClient = apiClientField.get(client);

        return (String) apiClient.getClass().getMethod("getBasePath").invoke(apiClient);
    }

    private static Clone.CloneBuilder<?, ?> cloneTask() {
        return Clone.builder()
            .id("clone")
            .type(Clone.class.getName())
            .url(Property.ofValue("https://github.com/kestra-io/plugin-git"));
    }

    @Test
    void shouldUseTheDefaultSdkAuthenticationUrlWhenTheTaskDoesNotSetOne() throws Exception {
        var task = cloneTask().build();

        assertThat(url(task.kestraClient(runContextWithSdkUrl(task, SDK_DEFAULT_URL))), is(SDK_DEFAULT_URL));
    }

    @Test
    void shouldUseTheDefaultSdkAuthenticationUrlForKestraApiTasks() throws Exception {
        var task = TenantSync.builder()
            .id("tenantSync")
            .type(TenantSync.class.getName())
            .url(Property.ofValue("https://github.com/kestra-io/plugin-git"))
            .branch(Property.ofValue("main"))
            .auth(AbstractKestraTask.Auth.builder().apiToken(Property.ofValue("token")).build())
            .build();

        assertThat(url(task.kestraClient(runContextWithSdkUrl(task, SDK_DEFAULT_URL))), is(SDK_DEFAULT_URL));
    }

    @Test
    void shouldPreferTheTaskUrlOverTheDefaultSdkAuthenticationUrl() throws Exception {
        var task = cloneTask().kestraUrl(Property.ofValue("https://task.example.com/")).build();

        assertThat(url(task.kestraClient(runContextWithSdkUrl(task, SDK_DEFAULT_URL))), is("https://task.example.com"));
    }

    @Test
    void shouldFallThroughWhenTheDefaultSdkAuthenticationUrlIsBlank() throws Exception {
        var task = cloneTask().build();

        assertThat(url(task.kestraClient(runContextWithSdkUrl(task, "   "))), is(DEFAULT_URL));
    }

    @Test
    void shouldIgnoreTheDefaultSdkAuthenticationUrlWhenAutoIsDisabled() throws Exception {
        var task = cloneTask()
            .auth(AbstractCloningTask.Auth.builder().auto(Property.ofValue(false)).build())
            .build();

        assertThat(url(task.kestraClient(runContextWithSdkUrl(task, SDK_DEFAULT_URL))), is(DEFAULT_URL));
    }
    /** The SDK builder defaults to Basic auth, so building a client without credentials would send `Basic base64("null:null")`. */
    @Test
    void shouldFailWhenAutoIsDisabledWithoutCredentials() throws Exception {
        var task = TenantSync.builder()
            .id("tenantSync")
            .type(TenantSync.class.getName())
            .url(Property.ofValue("https://github.com/kestra-io/plugin-git"))
            .branch(Property.ofValue("main"))
            .auth(AbstractKestraTask.Auth.builder().auto(Property.ofValue(false)).build())
            .build();

        RunContext runContext = runContextWithSdkUrl(task, SDK_DEFAULT_URL);

        assertThrows(IllegalArgumentException.class, () -> task.kestraClient(runContext));
    }
}
