package io.kestra.plugin.git;

import java.util.Optional;
import java.util.function.Predicate;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.SDK;

import jakarta.annotation.Nullable;

/**
 * The Kestra API endpoint a task talks to, together with the default SDK authentication it was resolved from.
 *
 * <p>
 * Both are read from the same place, so that an instance, tenant or namespace default covers the URL as well as
 * the credentials.
 */
final class KestraApiConnection {
    private static final String DEFAULT_URL = "http://localhost:8080";
    private static final String URL_TEMPLATE = "{{ kestra.url }}";

    private final String url;
    private final DefaultAuthSupplier defaultAuth;

    private KestraApiConnection(String url, DefaultAuthSupplier defaultAuth) {
        this.url = url;
        this.defaultAuth = defaultAuth;
    }

    /** The API URL, without any trailing slash. */
    String url() {
        return url;
    }

    /** The default SDK authentication, empty when the task opted out of it with {@code auth.auto}. */
    Optional<SDK.Auth> defaultAuth() {
        return defaultAuth.get();
    }

    /**
     * Resolves the API URL from, in order: the task's own {@code kestraUrl}, the default SDK authentication
     * (namespace then tenant then instance configuration, on the Enterprise Edition), the {@code kestra.url}
     * configuration property, and finally {@code http://localhost:8080}.
     *
     * <p>{@code auth.auto} opts out of the default authentication for the URL as well as for the credentials.
     */
    static KestraApiConnection resolve(RunContext runContext, @Nullable Property<String> kestraUrl, @Nullable KestraApiAuth auth) throws IllegalVariableEvaluationException {
        boolean rAuto = runContext.render(Optional.ofNullable(auth).map(KestraApiAuth::getAuto).orElse(null))
            .as(Boolean.class)
            .orElse(Boolean.TRUE);
        DefaultAuthSupplier defaultAuth = new DefaultAuthSupplier(runContext, rAuto);

        String rUrl = runContext.render(kestraUrl).as(String.class)
            .filter(Predicate.not(String::isBlank))
            .or(() -> defaultAuth.get().flatMap(SDK.Auth::url).filter(Predicate.not(String::isBlank)))
            .orElseGet(() -> configuredUrl(runContext));

        return new KestraApiConnection(rUrl.trim().replaceAll("/+$", ""), defaultAuth);
    }

    private static String configuredUrl(RunContext runContext) {
        try {
            String rUrl = runContext.render(URL_TEMPLATE);
            return rUrl == null || rUrl.isBlank() ? DEFAULT_URL : rUrl;
        } catch (IllegalVariableEvaluationException e) {
            return DEFAULT_URL;
        }
    }

    /** Looks the defaults up at most once, since on the Enterprise Edition it reads the namespace and tenant metastores. */
    private static final class DefaultAuthSupplier {
        private final RunContext runContext;
        private final boolean enabled;
        private Optional<SDK.Auth> resolved;

        private DefaultAuthSupplier(RunContext runContext, boolean enabled) {
            this.runContext = runContext;
            this.enabled = enabled;
        }

        private Optional<SDK.Auth> get() {
            if (resolved == null) {
                SDK sdk = enabled ? runContext.sdk() : null;
                resolved = Optional.ofNullable(sdk).map(SDK::defaultAuthentication).orElse(Optional.empty());
            }
            return resolved;
        }
    }
}
