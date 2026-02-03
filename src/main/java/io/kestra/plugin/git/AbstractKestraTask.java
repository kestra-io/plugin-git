package io.kestra.plugin.git;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.sdk.KestraClient;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Optional;

@SuperBuilder
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Getter
public abstract class AbstractKestraTask extends AbstractGitTask {
    private static final String DEFAULT_KESTRA_URL = "http://localhost:8080";
    private static final String KESTRA_URL_TEMPLATE = "{{ kestra.url }}";

    @Schema(title = "Kestra API URL. If null, uses 'kestra.url' from [configuration](https://kestra.io/docs/configuration#kestra-url). If that is also null, defaults to 'http://localhost:8080'.")
    private Property<String> kestraUrl;

    @Schema(title = "Authentication information")
    @NotNull
    private Auth auth;

    protected KestraClient kestraClient(RunContext runContext) throws IllegalVariableEvaluationException {
        // use the kestraUrl property if set, otherwise the config value, or else the default
        String rKestraUrl = runContext.render(kestraUrl).as(String.class)
            .orElseGet(() -> {
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
            Optional<String> maybeUsername = runContext.render(auth.username).as(String.class);
            Optional<String> maybePassword = runContext.render(auth.password).as(String.class);
            if (maybeUsername.isPresent() && maybePassword.isPresent()) {
                builder.basicAuth(maybeUsername.get(), maybePassword.get());
                return builder.build();
            }
            throw new IllegalArgumentException("Both username and password are required for HTTP Basic authentication");
        } else {
            throw new IllegalArgumentException("Auth is required");
        }
    }

    @Builder
    @Getter
    public static class Auth {
        @Schema(title = "Username for HTTP Basic authentication.")
        private Property<String> username;

        @Schema(title = "Password for HTTP Basic authentication.")
        private Property<String> password;
    }
}
