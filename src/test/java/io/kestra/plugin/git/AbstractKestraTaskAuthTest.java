package io.kestra.plugin.git;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.git.AbstractKestraTask.Auth;
import io.kestra.sdk.KestraClient;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class AbstractKestraTaskAuthTest {

    @Inject
    private RunContextFactory runContextFactory;

    private TenantSync taskWith(Auth auth) {
        return TenantSync.builder().auth(auth).build();
    }

    private RunContext runContext() {
        return runContextFactory.of(Collections.emptyMap());
    }

    /** Reads the Authorization header set on the built client (the SDK does not expose it). */
    private static String authorizationHeader(KestraClient client) throws Exception {
        Field apiClientField = KestraClient.class.getDeclaredField("apiClient");
        apiClientField.setAccessible(true);
        Object apiClient = apiClientField.get(client);

        Field headersField = apiClient.getClass().getDeclaredField("defaultHeaderMap");
        headersField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) headersField.get(apiClient);

        return headers.get("Authorization");
    }

    @Test
    void apiToken_usesBearer() throws Exception {
        var task = taskWith(Auth.builder().apiToken(Property.ofValue("my-token")).build());

        KestraClient client = task.kestraClient(runContext());

        assertThat(authorizationHeader(client), is("Bearer my-token"));
    }

    @Test
    void usernameAndPassword_usesBasic() throws Exception {
        var task = taskWith(
            Auth.builder()
                .username(Property.ofValue("user"))
                .password(Property.ofValue("pass"))
                .build()
        );

        KestraClient client = task.kestraClient(runContext());

        assertThat(authorizationHeader(client), startsWith("Basic "));
    }

    @Test
    void apiTokenWithUsername_isRejected() {
        var task = taskWith(
            Auth.builder()
                .apiToken(Property.ofValue("my-token"))
                .username(Property.ofValue("user"))
                .build()
        );

        var e = assertThrows(IllegalArgumentException.class, () -> task.kestraClient(runContext()));
        assertThat(e.getMessage(), is("Cannot use both API Token authentication and HTTP Basic authentication"));
    }

    @Test
    void apiTokenWithPassword_isRejected() {
        var task = taskWith(
            Auth.builder()
                .apiToken(Property.ofValue("my-token"))
                .password(Property.ofValue("pass"))
                .build()
        );

        var e = assertThrows(IllegalArgumentException.class, () -> task.kestraClient(runContext()));
        assertThat(e.getMessage(), is("Cannot use both API Token authentication and HTTP Basic authentication"));
    }

    @Test
    void onlyUsername_isRejected() {
        var task = taskWith(Auth.builder().username(Property.ofValue("user")).build());

        var e = assertThrows(IllegalArgumentException.class, () -> task.kestraClient(runContext()));
        assertThat(e.getMessage(), is("Both username and password are required for HTTP Basic authentication"));
    }

    @Test
    void onlyPassword_isRejected() {
        var task = taskWith(Auth.builder().password(Property.ofValue("pass")).build());

        var e = assertThrows(IllegalArgumentException.class, () -> task.kestraClient(runContext()));
        assertThat(e.getMessage(), is("Both username and password are required for HTTP Basic authentication"));
    }
}
