package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.services.FlowService;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.KestraIgnore;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.*;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Synchronizes the code for Namespace Files and flows based on the current state in Git.",
    description = "Replaced by [SyncFlows](https://kestra.io/plugins/plugin-git/tasks/io.kestra.plugin.git.syncflows) and [SyncNamespaceFiles](https://kestra.io/plugins/plugin-git/tasks/io.kestra.plugin.git.syncnamespacefiles). Files located in `gitDirectory` will be synced with namespace files under `namespaceFilesDirectory` folder. " +
        "Any file not present in the `gitDirectory` but present in `namespaceFilesDirectory` will be deleted from namespace files to ensure that Git remains a single source of truth for your workflow and application code. " +
        "If you don't want some files from Git to be synced, you can add them to a `.kestraignore` file at the root of your `gitDirectory` folder — that file works the same way as `.gitignore`. \n\n" +
        "If there is a `_flows` folder under the `gitDirectory` folder, any file within it will be parsed and imported as a flow under the namespace declared in the task. It's important to keep in mind that all flows **must be located within the same directory without any nested directories**. If you want to deploy all flows to kestra from Git using the Git Sync pattern, you have to place all your flows in the `_flows` directory. Adding namespace folders will result in an error and that's expected. Flows are not equivalent to Namespace Files — while Namespace Files can be stored in arbitrarily nested folders stored in Internal Storage, Flows are just metadata. Flows are sent to Kestra's API and stored in the database backend. This is why they follow a different deployment pattern and cannot be stored in nested folders in Git. \n\n" +
        "Another important aspect is that the namespace defined in the flow code **might get overwritten (!)** if the namespace defined within Git doesn't match the namespace or a child namespace defined in the Git Sync task. All Git deployments, both the Git Sync and Kestra's CI/CD integrations, operate on a namespace level to ensure namespace-level governance of permissions, secrets, and to allow separation of resources. If you leverage multiple namespaces in a monorepo, you can create multiple flows, each using the Git Sync task to sync specific Git directories to the desired namespaces."
)
@Plugin(
    examples = {
        @Example(
            title = "Synchronizes namespace files and flows based on the current state in a Git repository. This flow can run either on a schedule (using the Schedule trigger) or anytime you push a change to a given Git branch (using the Webhook trigger).",
            full = true,
            code = """
                id: sync_from_git
                namespace: company.team

                tasks:
                  - id: git
                    type: io.kestra.plugin.git.Sync
                    url: https://github.com/kestra-io/scripts
                    branch: main
                    username: git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    gitDirectory: your_git_dir # optional, otherwise all files
                    namespaceFilesDirectory: your_namespace_files_location # optional, otherwise the namespace root directory
                    dryRun: true  # if true, print the output of what files will be added/modified or deleted without overwriting the files yet

                triggers:
                  - id: every_minute
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "*/1 * * * *"
                """
        )
    }
)
public class Sync extends AbstractCloningTask implements RunnableTask<VoidOutput> {
    public static final String FLOWS_DIRECTORY = "_flows";
    public static final Pattern NAMESPACE_FINDER_PATTERN = Pattern.compile("(?m)^namespace: (.*)$");
    public static final Pattern FLOW_ID_FINDER_PATTERN = Pattern.compile("(?m)^id: (.*)$");

    @Schema(
        title = "Git directory to sync code from. If not specified, all files from a Git repository will be synchronized."
    )
    private Property<String> gitDirectory;

    @Schema(
        title = "Namespace files directory to which files from Git should be synced. It defaults to the root directory of the namespace."
    )
    private Property<String> namespaceFilesDirectory;

    private Property<String> branch;

    @Schema(
        title = "If true, the task will only display modifications without syncing any files yet. If false (default), all namespace files and flows will be overwritten based on the state in Git."
    )
    private Property<Boolean> dryRun;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        Map<String, String> flowProps = (Map<String, String>) runContext.getVariables().get("flow");
        String namespace = flowProps.get("namespace");
        String tenantId = flowProps.get("tenantId");
        boolean dryRun = this.dryRun != null && runContext.render(this.dryRun).as(Boolean.class).orElse(false);

        Clone clone = Clone.builder()
            .depth(1)
            .url(this.url)
            .branch(this.branch)
            .username(this.username)
            .password(this.password)
            .privateKey(this.privateKey)
            .passphrase(this.passphrase)
            .cloneSubmodules(this.cloneSubmodules)
            .build();

        clone.run(runContext);

        // we should synchronize git flows with current namespace flows
        Path absoluteGitDirPath = runContext.workingDir().resolve(runContext.render(this.gitDirectory).as(String.class).map(Path::of).orElse(null));
        Path flowsDirectoryBasePath = absoluteGitDirPath.resolve(FLOWS_DIRECTORY);
        KestraIgnore kestraIgnore = new KestraIgnore(absoluteGitDirPath);

        // synchronize flows directory to namespace flows
        File flowsDirectory = flowsDirectoryBasePath.toFile();
        if (flowsDirectory.exists()) {
            FlowRepositoryInterface flowRepository = ((DefaultRunContext)runContext).getApplicationContext().getBean(FlowRepositoryInterface.class);
            FlowService flowService = ((DefaultRunContext)runContext).getApplicationContext().getBean(FlowService.class);

            Set<String> flowIdsImported = Arrays.stream(flowsDirectory.listFiles())
                .map(File::toPath)
                .filter(filePath -> !kestraIgnore.isIgnoredFile(absoluteGitDirPath.relativize(filePath).toString(), true))
                .map(throwFunction(Files::readAllBytes))
                .map(String::new)
                .map(flowSource -> {
                    Matcher matcher = NAMESPACE_FINDER_PATTERN.matcher(flowSource);
                    matcher.find();
                    String previousNamespace = matcher.group(1);
                    if (previousNamespace.startsWith(namespace + ".")) {
                        return Map.entry(previousNamespace, flowSource);
                    }

                    return Map.entry(namespace, matcher.replaceFirst("namespace: " + namespace));
                })
                .map(throwFunction(flowSourceByNamespace -> {
                    boolean isAddition;
                    FlowWithSource flowWithSource;
                    String flowSource = flowSourceByNamespace.getValue();
                    if (dryRun) {
                        Matcher matcher = FLOW_ID_FINDER_PATTERN.matcher(flowSource);
                        matcher.find();
                        String flowId = matcher.group(1).trim();
                        isAddition = flowRepository.findById(tenantId, flowSourceByNamespace.getKey(), flowId).isEmpty();
                        flowWithSource = FlowWithSource.builder().source(flowSource).id(flowId).build();
                    } else {
                        flowWithSource = flowService.importFlow(tenantId, flowSource);
                        isAddition = flowWithSource.getRevision() == 1;
                    }

                    if (isAddition) {
                        logAddition(logger, "/_flows/" + flowWithSource.getId() + ".yml");
                    } else {
                        logUpdate(logger, "/_flows/" + flowWithSource.getId() + ".yml");
                    }

                    return flowWithSource;
                }))
                .map(FlowWithSource::getId)
                .collect(Collectors.toSet());

            // prevent self deletion
            flowIdsImported.add(flowProps.get("id"));

            flowRepository.findByNamespaceWithSource(tenantId, namespace).stream()
                .filter(flow -> !flowIdsImported.contains(flow.getId()))
                .forEach(flow -> {
                    if (!dryRun) {
                        flowRepository.delete(flow);
                    }
                    logDeletion(logger, "/_flows/" + flow.getId() + ".yml");
                });
        }

        Map<String, String> gitContentByFilePath;
        try (Stream<Path> walk = Files.walk(absoluteGitDirPath)) {
            List<Path> list = walk
                .filter(path -> {
                    String pathStr = path.toString();
                    return !pathStr.equals(absoluteGitDirPath.toString()) &&
                        !pathStr.contains("/.git/") && !pathStr.endsWith("/.git") &&
                        !pathStr.contains("/" + FLOWS_DIRECTORY + "/") && !pathStr.endsWith("/" + FLOWS_DIRECTORY) &&
                        !kestraIgnore.isIgnoredFile(pathStr, true);
                })
                .toList();
            gitContentByFilePath = list.stream()
                .map(Path::toFile)
                .collect(HashMap::new, throwBiConsumer((map, file) -> {
                    String relativePath = absoluteGitDirPath.relativize(file.toPath()).toString();
                    map.put(
                        "/" + (file.isDirectory() ? relativePath + "/" : relativePath),
                        file.isDirectory() ? null : FileUtils.readFileToString(file, "UTF-8")
                    );
                }), HashMap::putAll);
        }

        StorageInterface storage = ((DefaultRunContext)runContext).getApplicationContext().getBean(StorageInterface.class);
        URI namespaceFilePrefix = URI.create("kestra://" + StorageContext.namespaceFilePrefix(namespace) + "/");
        if (this.namespaceFilesDirectory != null) {
            String renderedNamespaceFilesDirectory = runContext.render(this.namespaceFilesDirectory).as(String.class).orElseThrow();
            renderedNamespaceFilesDirectory = renderedNamespaceFilesDirectory.startsWith("/") ? renderedNamespaceFilesDirectory.substring(1) : renderedNamespaceFilesDirectory;
            renderedNamespaceFilesDirectory = renderedNamespaceFilesDirectory.endsWith("/") ? renderedNamespaceFilesDirectory : renderedNamespaceFilesDirectory + "/";
            namespaceFilePrefix = namespaceFilePrefix.resolve(renderedNamespaceFilesDirectory);
        }
        URI finalNamespaceFilePrefix = namespaceFilePrefix;
        List<URI> namespaceFilesUris = storage.allByPrefix(tenantId, namespace, namespaceFilePrefix, true);

        Map<String, URI> fullUriByRelativeNsFilesPath = namespaceFilesUris.stream()
            .collect(Collectors.toMap(
                uri -> "/" + finalNamespaceFilePrefix.relativize(uri),
                Function.identity()
            ));

        logger.info("Dry run is {}, {}performing following actions (- for deletions, + for creations, ~ for update or no modification):", dryRun ? "enabled" : "disabled", dryRun ? "not " : "");
        // perform all required deletions before-hand
        fullUriByRelativeNsFilesPath.forEach(throwBiConsumer((relativeNsFilePath, uri) -> {
            if (!gitContentByFilePath.containsKey(relativeNsFilePath)) {
                logDeletion(logger, relativeNsFilePath);
                if (!dryRun) {
                    storage.delete(tenantId, namespace, uri);
                }
            }
        }));

        // perform all required additions/updates
        gitContentByFilePath.entrySet().stream()
            .sorted(Comparator.comparing(e -> StringUtils.countMatches(e.getKey(), "/")))
            .forEach(throwConsumer(contentByFilePath -> {
                String path = contentByFilePath.getKey();
                if (fullUriByRelativeNsFilesPath.containsKey(path)) {
                    logUpdate(logger, path);
                } else {
                    logAddition(logger, path);
                }

                if (!dryRun) {
                    URI fileUri = finalNamespaceFilePrefix.resolve(path.replace("\\","/").substring(1));
                    if (contentByFilePath.getValue() == null) {
                        storage.createDirectory(tenantId, namespace, fileUri);
                    } else {
                        storage.put(
                            tenantId,
                            namespace,
                            fileUri,
                            new ByteArrayInputStream(contentByFilePath.getValue().getBytes())
                        );
                    }
                }

            }));

        return null;
    }

    private static void logDeletion(Logger logger, String path) {
        logger.info("- {}", path);
    }

    private static void logAddition(Logger logger, String path) {
        logger.info("+ {}", path);
    }

    private static void logUpdate(Logger logger, String path) {
        logger.info("~ {}", path);
    }

    @Override
    @NotNull
    public Property<String> getUrl() {
        return super.getUrl();
    }
}
