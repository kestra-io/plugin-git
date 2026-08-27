package io.kestra.plugin.git;

import java.util.Optional;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.sdk.KestraClient;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@SuperBuilder
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Getter
public abstract class AbstractKestraTask extends AbstractGitTask {
    private static final String DEFAULT_KESTRA_URL = "http://localhost:8080";
    private static final String KESTRA_URL_TEMPLATE = "{{ kestra.url }}";

    @Schema(
        title = "Kestra API URL. If null, uses 'kestra.url' from [configuration](https://kestra.io/docs/configuration#kestra-url). If that is also null, defaults to 'http://localhost:8080'"
    )
    @PluginProperty(group = "connection")
    private Property<String> kestraUrl;

    @Schema(title = "Authentication information")
    @NotNull
    @PluginProperty(group = "main")
    private Auth auth;

    protected KestraClient kestraClient(RunContext runContext) throws IllegalVariableEvaluationException {
        // use the kestraUrl property if set, otherwise the config value, or else the default
        String rKestraUrl = runContext.render(kestraUrl).as(String.class)
            .orElseGet(() ->
            {
                try {
                    return runContext.render(KESTRA_URL_TEMPLATE);
                } catch (IllegalVariableEvaluationException e) {
                    return DEFAULT_KESTRA_URL;
                }
            });

        runContext.logger().debug("Kestra URL: {}", rKestraUrl);

        String normalizedUrl = rKestraUrl.trim().replaceAll("/+$", "");

        var builder = KestraClient.builder();
        builder.url(normalizedUrl);
        if (auth != null) {
            if (auth.apiToken != null && (auth.username != null || auth.password != null)) {
                throw new IllegalArgumentException("Cannot use both API Token authentication and HTTP Basic authentication");
            }

            var rApiToken = runContext.render(auth.apiToken).as(String.class).orElse(null);
            if (rApiToken != null) {
                builder.tokenAuth(rApiToken);
                return builder.build();
            }

            Optional<String> maybeUsername = runContext.render(auth.username).as(String.class);
            Optional<String> maybePassword = runContext.render(auth.password).as(String.class);
            if (maybeUsername.isPresent() && maybePassword.isPresent()) {
                builder.basicAuth(maybeUsername.get(), maybePassword.get());
                return builder.build();
            }
            if (maybeUsername.isPresent() || maybePassword.isPresent()) {
                throw new IllegalArgumentException("Both username and password are required for HTTP Basic authentication");
            }

            return authenticateWithDefaultsOrThrow(
                runContext, builder,
                runContext.render(auth.auto).as(Boolean.class).orElse(Boolean.TRUE)
            );
        }

        // try automatic authentication
        return authenticateWithDefaultsOrThrow(runContext, builder, true);
    }

    @Builder
    @Getter
    @Jacksonized
    public static class Auth {
        @Schema(title = "API token for Bearer authentication")
        @PluginProperty(secret = true, group = "connection")
        private Property<String> apiToken;

        @Schema(title = "Username for HTTP Basic authentication")
        @PluginProperty(secret = true, group = "connection")
        private Property<String> username;

        @Schema(title = "Password for HTTP Basic authentication")
        @PluginProperty(secret = true, group = "connection")
        private Property<String> password;

        @Schema(
            title = "Automatically retrieve credentials from Kestra's configuration if available",
            description = """
                The default configuration can be configured globally inside the Kestra configuration file:
                - Set `kestra.tasks.sdk.authentication.api-token` to use an API token
                - Set `kestra.tasks.sdk.authentication.username` and `kestra.tasks.sdk.authentication.password` for HTTP basic authentication
                The Enterprise edition also provides setting a default configuration at the Namespace of Tenant level by an administrator."""
        )
        @Builder.Default
        @PluginProperty(group = "advanced")
        private Property<Boolean> auto = Property.ofValue(Boolean.TRUE);
    }
}
