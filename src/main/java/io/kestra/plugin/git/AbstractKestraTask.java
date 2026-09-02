package io.kestra.plugin.git;

import java.util.Optional;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.SDK;
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
    @Schema(
        title = "Kestra API URL",
        description = """
            URL of the Kestra server API.
            If not set, the URL of the default SDK authentication is used, set with the `kestra.tasks.sdk.authentication.url` \
            configuration property, or at the namespace or the tenant level on the Enterprise Edition.
            It then falls back to the `kestra.url` configuration property, and finally to `http://localhost:8080`."""
    )
    @PluginProperty(group = "connection")
    protected Property<String> kestraUrl;

    @Schema(title = "Authentication information")
    @NotNull
    @PluginProperty(group = "main")
    private Auth auth;

    protected KestraClient kestraClient(RunContext runContext) throws IllegalVariableEvaluationException {
        KestraApiConnection connection = KestraApiConnection.resolve(runContext, kestraUrl, auth);

        runContext.logger().debug("Kestra URL: {}", connection.url());

        var builder = KestraClient.builder();
        builder.url(connection.url());
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

            Optional<KestraClient> autoAuthenticated = applyDefaultCredentials(builder, connection.defaultAuth());
            if (autoAuthenticated.isPresent()) {
                return autoAuthenticated.get();
            }

            throw new IllegalArgumentException(
                "No authentication method provided. Set 'auth.apiToken', or 'auth.username' and 'auth.password', or configure a default one with the 'kestra.tasks.sdk.authentication' properties."
            );
        } else {
            // try automatic authentication
            Optional<KestraClient> autoAuthenticated = applyDefaultCredentials(builder, connection.defaultAuth());
            if (autoAuthenticated.isPresent()) {
                return autoAuthenticated.get();
            }
        }
        return builder.build();
    }

    private static Optional<KestraClient> applyDefaultCredentials(KestraClient.KestraClientBuilder builder, Optional<SDK.Auth> defaultAuth) {
        return defaultAuth.map(defaults ->
        {
            if (defaults.apiToken().isPresent()) {
                return builder.tokenAuth(defaults.apiToken().get()).build();
            }
            if (defaults.username().isPresent() && defaults.password().isPresent()) {
                return builder.basicAuth(defaults.username().get(), defaults.password().get()).build();
            }
            return null;
        });
    }

    @Builder
    @Getter
    @Jacksonized
    public static class Auth implements KestraApiAuth {
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
            title = "Automatically retrieve the URL and the credentials from Kestra's configuration if available",
            description = """
                The default configuration can be configured globally inside the Kestra configuration file:
                - Set `kestra.tasks.sdk.authentication.url` to use a given API URL
                - Set `kestra.tasks.sdk.authentication.api-token` to use an API token
                - Set `kestra.tasks.sdk.authentication.username` and `kestra.tasks.sdk.authentication.password` for HTTP basic authentication
                The Enterprise edition also provides setting a default configuration at the Namespace of Tenant level by an administrator."""
        )
        @Builder.Default
        @PluginProperty(group = "advanced")
        private Property<Boolean> auto = Property.ofValue(Boolean.TRUE);
    }
}
