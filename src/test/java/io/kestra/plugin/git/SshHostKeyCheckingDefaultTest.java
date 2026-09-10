package io.kestra.plugin.git;

import java.lang.reflect.Field;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.TransportCommand;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * OSS has never verified the SSH host key by default; this must stay unchanged now that
 * {@code strictHostKeyChecking} is resolved through the shared lib's overridable
 * {@code defaultStrictHostKeyChecking()} hook (the Enterprise Edition overrides it to {@code true}).
 */
@KestraTest
class SshHostKeyCheckingDefaultTest {

    @Inject
    private RunContextFactory runContextFactory;

    /** Neither {@code TransportCommand} nor the SSH callback expose the resolved value via a public getter. */
    private static boolean resolvedStrictHostKeyChecking(TransportCommand<?, ?> command) throws Exception {
        Field configCallbackField = TransportCommand.class.getDeclaredField("transportConfigCallback");
        configCallbackField.setAccessible(true);
        Object callback = configCallbackField.get(command);

        Field strictField = callback.getClass().getDeclaredField("strictHostKeyChecking");
        strictField.setAccessible(true);
        return (boolean) strictField.get(callback);
    }

    @Test
    void clone_defaultsToNotVerifyingTheHostKey() throws Exception {
        var task = Clone.builder().privateKey(Property.ofValue("dummy-pem-content")).build();

        var command = task.authentified(Git.lsRemoteRepository(), runContextFactory.of());

        assertThat(resolvedStrictHostKeyChecking(command), is(false));
    }

    @Test
    void clone_canOptIntoVerifyingTheHostKey() throws Exception {
        var task = Clone.builder()
            .privateKey(Property.ofValue("dummy-pem-content"))
            .strictHostKeyChecking(Property.ofValue(true))
            .build();

        var command = task.authentified(Git.lsRemoteRepository(), runContextFactory.of());

        assertThat(resolvedStrictHostKeyChecking(command), is(true));
    }
}
