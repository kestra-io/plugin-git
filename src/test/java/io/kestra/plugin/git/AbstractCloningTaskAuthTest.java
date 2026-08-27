package io.kestra.plugin.git;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class AbstractCloningTaskAuthTest {

    @Inject
    private RunContextFactory runContextFactory;

    private RunContext runContext() {
        return runContextFactory.of(Collections.emptyMap());
    }

    @Test
    void noAuthAndNoDefaultAuthentication_failsFast() {
        // auto=false so the test stays deterministic even when default credentials are configured
        var task = NamespaceSync.builder()
            .auth(
                AbstractCloningTask.Auth.builder()
                    .auto(io.kestra.core.models.property.Property.ofValue(false))
                    .build()
            )
            .build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.kestraClient(runContext()));

        assertThat(e.getMessage(), containsString("No authentication method provided for the Kestra API"));
    }
}
