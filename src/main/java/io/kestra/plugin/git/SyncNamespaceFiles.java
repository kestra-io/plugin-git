package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.Namespace;
import io.kestra.core.storages.NamespaceFile;
import io.kestra.core.utils.WindowsUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
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
            title = "Sync all flows and scripts for selected namespaces from Git to Kestra every full hour. Note that this is a [System Flow](https://kestra.io/docs/concepts/system-flows), so make sure to adjust the Scope to SYSTEM in the UI filter to see this flow or its executions.",
            full = true,
            code = """
                id: git_sync
                namespace: system

                tasks:
                  - id: sync
                    type: io.kestra.plugin.core.flow.ForEach
                    values: ["company", "company.team", "company.analytics"]
                    tasks:
                      - id: flows
                        type: io.kestra.plugin.git.SyncFlows
                        targetNamespace: "{{ taskrun.value }}"
                        gitDirectory: "{{'flows/' ~ taskrun.value}}"
                        includeChildNamespaces: false

                      - id: scripts
                        type: io.kestra.plugin.git.SyncNamespaceFiles
                        namespace: "{{ taskrun.value }}"
                        gitDirectory: "{{'scripts/' ~ taskrun.value}}"

                pluginDefaults:
                  - type: io.kestra.plugin.git
                    values:
                      username: anna-geller
                      url: https://github.com/anna-geller/product
                      password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                      branch: main
                      dryRun: false

                triggers:
                  - id: every_full_hour
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 * * * *"
                """
        ),
        @Example(
            title = "Sync Namespace Files from a Git repository. This flow can run either on a schedule (using the Schedule trigger) or anytime you push a change to a given Git branch (using the Webhook trigger).",
            full = true,
            code = """
                id: sync_from_git
                namespace: system

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

                triggers:
                  - id: every_minute
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "*/1 * * * *"
                """
        )
    }
)
public class SyncNamespaceFiles extends AbstractSyncTask<URI, SyncNamespaceFiles.Output> {
    @Schema(
        title = "The branch from which Namespace Files will be synced to Kestra."
    )
    @Builder.Default
    private Property<String> branch = Property.of("main");

    @Schema(
        title = "The namespace from which files should be synced from the `gitDirectory` to Kestra."
    )
    @Builder.Default
    private Property<String> namespace = new Property<>("{{ flow.namespace }}");

    @Schema(
        title = "Directory from which Namespace Files should be synced.",
        description = "If not set, this task assumes your branch includes a directory named `_files`"
    )
    @Builder.Default
    private Property<String> gitDirectory = Property.of("_files");

    @Schema(
        title = "Whether you want to delete Namespace Files present in kestra but not present in Git.",
        description = "It’s `false` by default to avoid destructive behavior. Use with caution because when set to `true`, this task will delete all Namespace Files which are not present in Git."
    )
    @Builder.Default
    private Property<Boolean> delete = Property.of(false);

    @Override
    public Property<String> fetchedNamespace() {
        return this.namespace;
    }

    @Override
    protected void deleteResource(RunContext runContext, String renderedNamespace, URI instanceUri) throws IOException {
        runContext.storage().namespace(renderedNamespace).delete(Path.of(instanceUri.getPath().replace("\\","/")));
    }

    @Override
    protected URI simulateResourceWrite(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) {
        return NamespaceFile.of(renderedNamespace, uri).uri();
    }

    @Override
    protected URI writeResource(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException, URISyntaxException {
        Namespace namespace = runContext.storage().namespace(renderedNamespace);

        try {
            return inputStream == null ?
                URI.create(namespace.createDirectory(Path.of(uri.getPath())) + "/") :
                namespace.putFile(Path.of(uri.getPath()), inputStream).uri();
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
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
            .map(namespaceFile -> namespaceFile.uri())
            .toList();
    }

    @Override
    protected URI toUri(String renderedNamespace, URI resource) {
        if (resource == null) {
            return null;
        }
        NamespaceFile namespaceFile = NamespaceFile.of(renderedNamespace, WindowsUtils.windowsToUnixURI(resource));
        String trailingSlash = namespaceFile.isDirectory() ? "/" : "";
        return URI.create(namespaceFile.path(true).toString().replace("\\", "/") + trailingSlash);
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
