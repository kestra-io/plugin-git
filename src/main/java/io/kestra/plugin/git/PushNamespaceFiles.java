package io.kestra.plugin.git;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.Namespace;
import io.kestra.core.utils.PathMatcherPredicate;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Commit and push Namespace Files created from Kestra’s UI to Git.",
    description = """
        Using this task, you can push one or more Namespace Files from a given kestra namespace to Git. Note that in contrast to `PushFlows`, this task requires pushing code for each namespace separately. You can use the `ForEach` task as shown below to loop over multiple namespaces. Check the [Version Control with Git](https://kestra.io/docs/developer-guide/git) guide for more examples.
        Git does not guarantee the order of push operations to a remote repository, which can lead to potential conflicts when multiple users or flows attempt to push changes simultaneously.
        To minimize the risk of data loss and merge conflicts, it is strongly recommended to use sequential workflows or push changes to separate branches."""
)
@Plugin(
    examples = {
        @Example(
            title = "Release all flows and scripts from selected namespaces to a Git repository every Thursday at 11:00 AM. Adjust the `values` list to include the namespaces for which you want to push your code to Git. This [System Flow](https://kestra.io/docs/concepts/system-flows) will create two commits per namespace: one for the flows and one for the scripts.",
            full = true,
            code = """
                id: git_push
                namespace: system

                tasks:
                  - id: push
                    type: io.kestra.plugin.core.flow.ForEach
                    values: ["company", "company.team", "company.analytics"]
                    tasks:
                      - id: flows
                        type: io.kestra.plugin.git.PushFlows
                        sourceNamespace: "{{ taskrun.value }}"
                        gitDirectory: "{{'flows/' ~ taskrun.value}}"
                        includeChildNamespaces: false

                      - id: scripts
                        type: io.kestra.plugin.git.PushNamespaceFiles
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
                  - id: schedule_push_to_git
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 11 * * 4"
                """
        ),
        @Example(
            title = "Push all saved Namespace Files from the dev namespace to a Git repository every 15 minutes.",
            full = true,
            code = """
                id: push_to_git
                namespace: system

                tasks:
                  - id: commit_and_push
                    type: io.kestra.plugin.git.PushNamespaceFiles
                    namespace: dev
                    files: "**"  # optional list of glob patterns; by default, all files are pushed
                    gitDirectory: _files # optional path in Git where Namespace Files should be pushed
                    url: https://github.com/kestra-io/scripts # required string
                    username: git_username # required string needed for Auth with Git
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    branch: dev # optional, uses "kestra" by default
                    commitMessage: "add namespace files" # optional string
                    dryRun: true  # if true, you'll see what files will be added, modified or deleted based on the state in Git without overwriting the files yet

                triggers:
                  - id: schedule_push_to_git
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "*/15 * * * *"
                """
        )
    }
)
public class PushNamespaceFiles extends AbstractPushTask<PushNamespaceFiles.Output> {
    @Schema(
        title = "The branch to which Namespace Files should be committed and pushed",
        description = "If the branch doesn’t exist yet, it will be created. If not set, the task will push the files to the `kestra` branch."
    )
    @Builder.Default
    private Property<String> branch = Property.ofValue("main");

    @Schema(
        title = "The namespace from which files should be pushed to the `gitDirectory`"
    )
    @Builder.Default
    private Property<String> namespace = new Property<>("{{ flow.namespace }}");

    @Schema(
        title = "Directory to which Namespace Files should be pushed.",
        description = """
            If not set, files will be pushed to a Git directory `named _files`. See the table below for an example mapping of Namespace Files to Git paths:

            |  Namespace File Path  |      Git directory path      |
            | --------------------- | ---------------------------- |
            | scripts/app.py        | _files/scripts/app.py        |
            | scripts/etl.py        | _files/scripts/etl.py        |
            | queries/orders.sql    | _files/queries/orders.sql    |
            | queries/customers.sql | _files/queries/customers.sql |
            | requirements.txt      | _files/requirements.txt      |"""
    )
    @Builder.Default
    private Property<String> gitDirectory = Property.ofValue("_files");

    @Schema(
        title = "Which Namespace Files should be included in the commit",
        description = """
            By default, Kestra will push all Namespace Files from the specified namespace.
            If you want to push only a specific file or directory (e.g., myfile.py), you can set it explicitly using files: myfile.py.
            Given that this is a glob pattern string (or a list of glob patterns), you can include as many files as you wish, provided that the user is authorized to access that namespace.
            Note that each glob pattern try to match the file name OR the relative path starting from `gitDirectory`""",
        defaultValue = "**"

    )
    @PluginProperty(dynamic = true)
    private Object files;

    @Schema(
        title = "Git commit message",
        defaultValue = "Add files from `namespace` namespace"
    )
    @Override
    public Property<String> getCommitMessage() {
        return Optional.ofNullable(this.commitMessage).orElse(new Property<>("Add files from " + this.namespace.toString() + " namespace"));
    }

    @Schema(
        title = "Fail the task if no files are matched",
        description = "If true, the task will fail explicitly when no files are matched by the provided 'files' property."
    )
    @Builder.Default
    private Property<Boolean> errorOnMissing = Property.ofValue(false);

    @Override
    public Object globs() {
        return this.files;
    }

    @Override
    public Property<String> fetchedNamespace() {
        return this.namespace;
    }

    @Override
    protected Map<Path, Supplier<InputStream>> instanceResourcesContentByPath(RunContext runContext, Path baseDirectory, List<String> globs) throws Exception {

        Namespace storage = runContext.storage().namespace(runContext.render(this.namespace).as(String.class).orElse(null));
        Predicate<Path> matcher = (globs != null) ? PathMatcherPredicate.matches(globs) : (path -> true);

        Map<Path, Supplier<InputStream>> filesMap = storage
            .findAllFilesMatching(matcher)
            .stream()
            .collect(Collectors.toMap(
                nsFile -> baseDirectory.resolve(nsFile.path(false)),
                throwFunction(nsFile -> throwSupplier(() -> storage.getFileContent(Path.of(nsFile.path()))))
            ));

        if (runContext.render(errorOnMissing).as(Boolean.class).orElse(false) && filesMap.isEmpty()) {
            throw new KestraRuntimeException("No Namespace Files matched the provided 'files' parameter to commit.");
        }

        return filesMap;
    }

    @Override
    protected Output output(AbstractPushTask.Output pushOutput, URI diffFileStorageUri) {
        return Output.builder()
            .commitId(pushOutput.getCommitId())
            .commitURL(pushOutput.getCommitURL())
            .files(diffFileStorageUri)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractPushTask.Output {
        @Schema(
            title = "A file containing all changes pushed (or not in case of dry run) to Git",
            description = """
                The output format is a ION file with one row per file, each row containing the number of added, deleted, and changed lines.
                A row looks as follows: `{changes:"3",file:"path/to/my/script.py",deletions:"-5",additions:"+10"}`"""
        )
        private URI files;

        @Override
        public URI diffFileUri() {
            return this.files;
        }
    }
}
