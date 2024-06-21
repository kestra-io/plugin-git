package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.Namespace;
import io.kestra.core.storages.NamespaceFile;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Sync Namespace Files from Git to Kestra.",
    description = """
        This task syncs Namespace Files from a given Git branch to a Kestra `namespace. If the delete property is set to true, any Namespace Files available in kestra but not present in the gitDirectory will be deleted, allowing to maintain Git as a single source of truth for your Namespace Files. Check the Version Control with Git documentation for more details.
        Using this task, you can push one or more Namespace Files from a given kestra namespace to Git. Check the [Version Control with Git](https://kestra.io/docs/developer-guide/git) documentation for more details."""
)
@Plugin(
    examples = {
        @Example(
            title = "Sync Namespace Files from a Git repository. This flow can run either on a schedule (using the Schedule trigger) or anytime you push a change to a given Git branch (using the Webhook trigger).",
            full = true,
            code = {
                """
                    id: sync_flows_from_git
                    namespace: company.team
                    \s
                    tasks:
                      - id: git
                        type: io.kestra.plugin.git.SyncNamespaceFiles
                        namespace: prod
                        gitDirectory: _files # optional; set to _files by default
                        delete: true # optional; by default, it's set to false to avoid destructive behavior
                        url: https://github.com/kestra-io/flows
                        branch: main
                        username: git_username
                        password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                        dryRun: true  # if true, the task will only log which flows from Git will be added/modified or deleted in kestra without making any changes in kestra backend yet
                    \s
                    triggers:
                      - id: every_minute
                        type: io.kestra.plugin.core.trigger.Schedule
                        cron: "*/1 * * * *\""""
            }
        )
    }
)
public class SyncNamespaceFiles extends AbstractSyncTask<URI, SyncNamespaceFiles.Output> {
    @Schema(
        title = "The branch from which Namespace Files will be synced to Kestra."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String branch = "kestra";

    @Schema(
        title = "The namespace from which files should be synced from the `gitDirectory` to Kestra."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String namespace = "{{ flow.namespace }}";

    @Schema(
        title = "Directory from which Namespace Files should be synced.",
        description = "If not set, this task assumes your branch includes a directory named `_files`"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String gitDirectory = "_files";

    @Schema(
        title = "Whether you want to delete Namespace Files present in kestra but not present in Git.",
        description = "It’s `false` by default to avoid destructive behavior. Use with caution because when set to `true`, this task will delete all Namespace Files which are not present in Git."
    )
    @PluginProperty
    @Builder.Default
    private boolean delete = false;

    @Override
    public String fetchedNamespace() {
        return this.namespace;
    }

    @Override
    protected void deleteResource(RunContext runContext, String renderedNamespace, URI instanceUri) throws IOException {
        runContext.storage().namespace(renderedNamespace).delete(Path.of(instanceUri.getPath()));
    }

    @Override
    protected URI simulateResourceWrite(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) {
        return NamespaceFile.of(renderedNamespace, uri).uri();
    }

    @Override
    protected URI writeResource(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException {
        Namespace namespace = runContext.storage().namespace(renderedNamespace);

        return inputStream == null ?
            URI.create(namespace.createDirectory(Path.of(uri.getPath())) + "/") :
            namespace.putFile(Path.of(uri.getPath()), inputStream).uri();
    }

    @Override
    protected SyncResult wrapper(RunContext runContext, String renderedGitDirectory, String renderedNamespace, URI resourceUri, URI resourceBeforeUpdate, URI resourceAfterUpdate) {
        SyncState syncState;
        if (resourceUri == null) {
            syncState = SyncState.DELETED;
        } else if (resourceBeforeUpdate == null) {
            syncState = SyncState.ADDED;
        } else {
            // here we don't want to query the content of the file to check if it has changed
            syncState = SyncState.OVERWRITTEN;
        }

        String kestraPath = Optional.ofNullable(this.toUri(
            runContext,
            renderedNamespace,
            resourceAfterUpdate == null ? resourceBeforeUpdate : resourceAfterUpdate
        )).map(URI::getPath).orElse(null);
        SyncResult.SyncResultBuilder<?, ?> builder = SyncResult.builder()
            .syncState(syncState)
            .kestraPath(kestraPath);

        if (syncState != SyncState.DELETED) {
            builder.gitPath(renderedGitDirectory + resourceUri);
        }

        return builder.build();
    }

    @Override
    protected List<URI> fetchResources(RunContext runContext, String renderedNamespace) throws IOException {
        return runContext.storage().namespace(renderedNamespace).all(true)
            .stream()
            .map(namespaceFile -> toUri(runContext, renderedNamespace, namespaceFile.uri()))
            .toList();
    }

    @Override
    protected URI toUri(RunContext runContext, String renderedNamespace, URI resource) {
        if (resource == null) {
            return null;
        }
        NamespaceFile namespaceFile = NamespaceFile.of(renderedNamespace, resource);
        String trailingSlash = namespaceFile.isDirectory() ? "/" : "";
        return URI.create(namespaceFile.path(true).toString() + trailingSlash);
    }

    @Override
    protected Output output(URI diffFileStorageUri) {
        return Output.builder()
            .files(diffFileStorageUri)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractSyncTask.Output {
        @Schema(
            title = "A file containing all changes applied (or not in case of dry run) from Git.",
            description = """
                The output format is a ION file with one row per files, each row containing the number of added, deleted and changed lines.
                A row looks as follows: `{changes:"3",file:"path/to/my/script.py",deletions:"-5",additions:"+10"}`"""
        )
        private URI files;

        @Override
        public URI diffFileUri() {
            return this.files;
        }
    }


    @SuperBuilder
    @Getter
    public static class SyncResult extends AbstractSyncTask.SyncResult {
        private String kestraPath;
    }
}
