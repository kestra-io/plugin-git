package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.NamespaceFilesService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

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
                                        namespace: system
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
                                            type: io.kestra.core.models.triggers.types.Schedule
                                            cron: "*/1 * * * *\""""
                        }
                )
        }
)
public class SyncNamespaceFiles extends AbstractSyncTask<SyncNamespaceFiles.Output> {
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
            description = """
                    If not set, this task assumes your branch includes a directory named `_files`"""
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String gitDirectory = "_files";

    @Override
    public String fetchedNamespace() {
        return this.namespace;
    }

    @Override
    protected void deleteResource(NamespaceFilesService namespaceFilesService, String tenantId, String renderedNamespace, URI instanceUri) throws IOException {
        namespaceFilesService.delete(tenantId, renderedNamespace, instanceUri);
    }

    @Override
    protected void writeResource(RunContext runContext, String tenantId, String renderedNamespace, URI uri, InputStream inputStream) throws IOException {
        NamespaceFilesService namespaceFilesService = runContext.getApplicationContext().getBean(NamespaceFilesService.class);
        if (inputStream == null) {
            namespaceFilesService.createDirectory(tenantId, renderedNamespace, uri);
        } else {
            try {
                namespaceFilesService.createFile(tenantId, renderedNamespace, uri, inputStream);
            } catch (FileNotFoundException e) {
                if (!this.isDelete()) {
                    if (e.getMessage().contains("Is directory")) {
                        runContext.logger().warn("Kestra already has a directory under {} and Git has a file with the same name. If you want to proceed with directory replacement with file, please add `delete: true` flag.", uri);
                    } else if (e.getMessage().contains("Not a directory")) {
                        String path = uri.getPath();
                        runContext.logger().warn("Kestra already has a file named {} and Git has a directory with the same name. If you want to proceed with file replacement with directory, please add `delete: true` flag.", path.substring(0, path.lastIndexOf("/")));
                    }
                    return;
                }

                throw e;
            }
        }
    }

    @Override
    protected SyncResult wrapper(String renderedGitDirectory, String localPath, SyncState syncState) {
        SyncResult.SyncResultBuilder<?, ?> builder = SyncResult.builder()
                .syncState(syncState)
                .kestraPath(localPath);

        if (syncState != SyncState.DELETED) {
            builder.gitPath(renderedGitDirectory + localPath);
        }

        return builder.build();
    }

    @Override
    protected List<URI> instanceFilledUris(RunContext runContext, String tenantId, String renderedNamespace) throws IOException {
        return runContext.getApplicationContext().getBean(NamespaceFilesService.class)
                .recursiveList(tenantId, renderedNamespace, null, true);
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
