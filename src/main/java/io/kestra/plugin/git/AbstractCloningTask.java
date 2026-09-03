package io.kestra.plugin.git;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevTag;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.TagOpt;
import org.slf4j.Logger;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.SDK;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.PagedResultsNamespace;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractCloningTask extends AbstractGitTask {
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

    @Schema(
        title = "Clone submodules",
        description = "Default false; enable to fetch and initialize nested submodules."
    )
    @PluginProperty(group = "advanced")
    protected Property<Boolean> cloneSubmodules;

    @Schema(title = "Kestra API authentication")
    @PluginProperty(group = "connection")
    protected Auth auth;

    protected KestraClient kestraClient(RunContext runContext) throws IllegalVariableEvaluationException {
        KestraApiConnection connection = KestraApiConnection.resolve(runContext, kestraUrl, auth);

        runContext.logger().debug("Kestra URL: {}", connection.url());

        var builder = KestraClient.builder().url(connection.url());

        if (auth != null) {
            Optional<String> maybeApiToken = runContext.render(auth.apiToken).as(String.class);
            Optional<String> maybeUsername = runContext.render(auth.username).as(String.class);
            Optional<String> maybePassword = runContext.render(auth.password).as(String.class);
            if (maybeApiToken.isPresent() && (maybeUsername.isPresent() || maybePassword.isPresent())) {
                throw new IllegalArgumentException("Use either apiToken or username/password for authentication, not both");
            }
            if (maybeApiToken.isPresent()) {
                return builder.tokenAuth(maybeApiToken.get()).build();
            }
            if (maybeUsername.isPresent() && maybePassword.isPresent()) {
                return builder.basicAuth(maybeUsername.get(), maybePassword.get()).build();
            }
            if (maybeUsername.isPresent() || maybePassword.isPresent()) {
                throw new IllegalArgumentException("Both username and password are required for HTTP Basic authentication");
            }
        }

        Optional<SDK.Auth> autoAuth = connection.defaultAuth();
        if (autoAuth.isPresent()) {
            if (autoAuth.get().username().isPresent() && autoAuth.get().password().isPresent()) {
                return builder.basicAuth(autoAuth.get().username().get(), autoAuth.get().password().get()).build();
            }
            if (autoAuth.get().apiToken().isPresent()) {
                return builder.tokenAuth(autoAuth.get().apiToken().get()).build();
            }
        }

        return builder.build();
    }

    protected List<String> descendantNamespaces(RunContext runContext, String tenantId, String namespace) throws IllegalVariableEvaluationException, ApiException {
        var client = kestraClient(runContext);
        List<String> out = new ArrayList<>();
        int page = 1;
        int size = 200;
        List<io.kestra.sdk.model.Namespace> results;
        do {
            // q is a server-side contains filter, so prefix matches are never missed
            PagedResultsNamespace result = client.namespaces().searchNamespaces(tenantId, namespace + ".", page, size, null, false);
            results = result.getResults();
            if (results == null) {
                break;
            }
            results.forEach(ns ->
            {
                if (isDescendant(namespace, ns.getId())) {
                    out.add(ns.getId());
                }
            });
            page++;
        } while (results.size() == size);
        return out;
    }

    protected static boolean isDescendant(String rootNamespace, String namespace) {
        return namespace != null && namespace.startsWith(rootNamespace + ".");
    }

    @Builder
    @Getter
    @Jacksonized
    public static class Auth implements KestraApiAuth {
        @Schema(title = "Username for HTTP Basic authentication.")
        @PluginProperty(secret = true, group = "connection")
        private Property<String> username;

        @Schema(title = "Password for HTTP Basic authentication.")
        @PluginProperty(secret = true, group = "connection")
        private Property<String> password;

        @Schema(title = "API token for authentication.")
        @PluginProperty(secret = true, group = "connection")
        private Property<String> apiToken;

        @Schema(
            title = "Automatically retrieve the URL and the credentials from Kestra's configuration if available",
            description = """
                Can be configured globally in the Kestra configuration file:
                - Set `kestra.tasks.sdk.authentication.url` for the API URL
                - Set `kestra.tasks.sdk.authentication.api-token` for API token auth
                - Set `kestra.tasks.sdk.authentication.username` and `kestra.tasks.sdk.authentication.password` for HTTP Basic auth
                The Enterprise Edition also allows an administrator to set these defaults at the namespace or the tenant level."""
        )
        @Builder.Default
        @PluginProperty(group = "advanced")
        private Property<Boolean> auto = Property.ofValue(Boolean.TRUE);
    }

    protected void checkoutCommit(Git git, String sha, Logger logger, boolean noTags) throws Exception {
        // Ensure we have a full history in case the repo was shallow by default on the remote
        // or if the requested SHA is deep in history.
        try {
            var refSpecs = new ArrayList<>(List.of(new RefSpec("+refs/heads/*:refs/remotes/origin/*")));
            if (!noTags) {
                refSpecs.add(new RefSpec("+refs/tags/*:refs/tags/*"));
            }

            var fetch = git.fetch().setRefSpecs(refSpecs);
            if (noTags) {
                fetch.setTagOpt(TagOpt.NO_TAGS);
            }

            fetch.call();
        } catch (Exception fetchEx) {
            logger.warn("Fetch before checkout failed: {}", fetchEx.getMessage());
        }

        ObjectId target;
        try {
            target = git.getRepository().resolve(sha);
            if (target == null) {
                throw new IllegalArgumentException("Cannot resolve commit SHA: " + sha);
            }
            git.checkout().setName(target.name()).call();
            logger.info("Checked out commit {} (detached HEAD)", target.name());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to checkout commit " + sha + ": " + e.getMessage(), e);
        }
    }

    protected void checkoutTag(Git git, String rTagName, Logger logger, boolean noTags) throws Exception {
        if (!noTags) {
            git.fetch()
                .setRefSpecs(new RefSpec("+refs/tags/*:refs/tags/*"))
                .call();
        }

        Ref tagRef = git.getRepository().findRef("refs/tags/" + rTagName);
        if (tagRef == null) {
            throw new IllegalArgumentException("Cannot resolve tag: " + rTagName);
        }

        ObjectId commitId;
        try (RevWalk revWalk = new RevWalk(git.getRepository())) {
            RevObject object = revWalk.parseAny(tagRef.getObjectId());

            if (object instanceof RevTag revTag) {
                // we have Annotated tag, need to peel to commit
                commitId = revTag.getObject();
            } else {
                // we have Lightweight tag which already points to commit
                commitId = object;
            }
        }

        git.checkout().setName(commitId.name()).call();

        logger.info("Checked out tag {} at commit {}", rTagName, commitId.name());
    }

    protected void checkoutBranch(Git git, String branch, Logger logger) throws Exception {
        if (branch != null) {
            git.checkout().setName(branch).call();
            logger.info("Checked out branch {}", branch);
        }
    }
}
