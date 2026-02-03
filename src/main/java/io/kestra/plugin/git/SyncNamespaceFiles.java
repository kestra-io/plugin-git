package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.Namespace;
import io.kestra.core.storages.NamespaceFile;
import io.kestra.core.storages.StorageContext;
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
    title = "Sync Namespace Files from Git",
    description = "Imports Namespace Files from a Git branch into a Kestra namespace. Can delete files missing in Git, honors `.kestraignore`, and supports dry-run diff output."
)
@Plugin(
    examples = {
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
                    gitDirectory: _files
                    delete: true
                    url: https://github.com/kestra-io/flows
                    branch: main
                    username: git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    dryRun: true

                triggers:
                  - id: every_minute
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "*/1 * * * *"
                """
        ),
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
        )
    }
)
public class SyncNamespaceFiles extends AbstractSyncTask<NamespaceFile, SyncNamespaceFiles.Output> {
    @Schema(
        title = "Branch to sync",
        description = "Defaults to `main`."
    )
    @Builder.Default
    private Property<String> branch = Property.ofValue("main");

    @Schema(
        title = "Target namespace",
        description = "Namespace receiving the files; defaults to the current flow namespace."
    )
    @Builder.Default
    private Property<String> namespace = new Property<>("{{ flow.namespace }}");

    @Schema(
        title = "Git directory for Namespace Files",
        description = "Relative path containing files; defaults to `_files`."
    )
    @Builder.Default
    private Property<String> gitDirectory = Property.ofValue("_files");

    @Schema(
        title = "Delete files missing in Git",
        description = "Default false. When true, removes Namespace Files absent from Git."
    )
    @Builder.Default
    private Property<Boolean> delete = Property.ofValue(false);

    @Override
    public Property<String> fetchedNamespace() {
        return this.namespace;
    }

    @Override
    protected void deleteResource(RunContext runContext, String renderedNamespace, NamespaceFile namespaceFile) throws IOException {
        runContext.storage().namespace(renderedNamespace).delete(namespaceFile);
    }

    @Override
    protected NamespaceFile
    simulateResourceWrite(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) {
        return NamespaceFile.of(renderedNamespace, uri);
    }

    @Override
    protected NamespaceFile writeResource(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException, URISyntaxException {
        Namespace namespace = runContext.storage().namespace(renderedNamespace);
        NamespaceFile.of(renderedNamespace, uri);
        try {
            return inputStream == null ?
                namespace.createDirectory(Path.of(uri.getPath())) :
                namespace.putFile(Path.of(uri.getPath()), inputStream).stream()
                    .filter(nf -> !nf.isDirectory())
                    .findFirst()
                    .orElseThrow();
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected SyncResult wrapper(RunContext runContext, String renderedGitDirectory, String renderedNamespace, URI resourceUri, NamespaceFile resourceBeforeUpdate, NamespaceFile resourceAfterUpdate) {
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
    protected List<NamespaceFile> fetchResources(RunContext runContext, String renderedNamespace) throws IOException {
        return runContext.storage().namespace(renderedNamespace).all();
    }

    @Override
    protected URI toUri(String renderedNamespace, NamespaceFile resource) {
        if (resource == null) {
            return null;
        }

        boolean hasTrailingSlash = resource.uri().toString().endsWith("/");

        String path = resource.path();
        if (hasTrailingSlash && !path.endsWith("/")) {
            path = path + "/";
        }

        return NamespaceFile.of(renderedNamespace, path, 1).uri();
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
            title = "Diff of synced Namespace Files",
            description = "ION file listing per-file sync actions (added, deleted, overwritten)."
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
