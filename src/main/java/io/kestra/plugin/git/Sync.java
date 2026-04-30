package io.kestra.plugin.git;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.KestraIgnore;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.QueryFilter;
import io.kestra.sdk.model.QueryFilterField;
import io.kestra.sdk.model.QueryFilterOp;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.*;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Deprecated(since = "1.0.0", forRemoval = true)
@Schema(
    deprecated = true,
    title = "Deprecated: sync flows and Namespace Files",
    description = "Superseded by SyncFlows and SyncNamespaceFiles. Syncs `_flows` (flows) plus other files from `gitDirectory` into a namespace, deleting extras unless excluded via `.kestraignore`. Flow files must live directly under `_flows` (no nesting)."
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
        title = "Git directory to sync from",
        description = "Defaults to repo root; `_flows` within it is used for flows."
    )
    @PluginProperty(group = "destination")
    private Property<String> gitDirectory;

    @Schema(
        title = "Target namespace files directory",
        description = "Optional subpath under the namespace; defaults to namespace root."
    )
    @PluginProperty(group = "source")
    private Property<String> namespaceFilesDirectory;

    @PluginProperty(group = "advanced")
    private Property<String> branch;

    @Schema(
        title = "Dry run only",
        description = "When true, logs planned additions/updates/deletions without applying."
    )
    @PluginProperty(group = "reliability")
    private Property<Boolean> dryRun;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        Map<String, String> flowProps = (Map<String, String>) runContext.getVariables().get("flow");
        String namespace = flowProps.get("namespace");
        String tenantId = flowProps.get("tenantId");
        boolean dryRun = this.dryRun != null && runContext.render(this.dryRun).as(Boolean.class).orElse(false);

        configureHttpTransport(runContext);

        // we add this method to configure ssl to allow self signed certs
        configureEnvironmentWithSsl(runContext);

        Clone clone = Clone.builder()
            .depth(Property.ofValue(1))
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
            KestraClient kestraClient = kestraClient(runContext);

            Set<String> flowIdsImported = Arrays.stream(flowsDirectory.listFiles())
                .map(File::toPath)
                .filter(filePath -> !kestraIgnore.isIgnoredFile(absoluteGitDirPath.relativize(filePath).toString(), true))
                .map(throwFunction(Files::readAllBytes))
                .map(String::new)
                .map(flowSource ->
                {
                    Matcher matcher = NAMESPACE_FINDER_PATTERN.matcher(flowSource);
                    matcher.find();
                    String previousNamespace = matcher.group(1);
                    if (previousNamespace.startsWith(namespace + ".")) {
                        return Map.entry(previousNamespace, flowSource);
                    }

                    return Map.entry(namespace, matcher.replaceFirst("namespace: " + namespace));
                })
                .map(throwFunction(flowSourceByNamespace ->
                {
                    boolean isAddition;
                    FlowWithSource flowWithSource;
                    String flowSource = flowSourceByNamespace.getValue();
                    if (dryRun) {
                        Matcher matcher = FLOW_ID_FINDER_PATTERN.matcher(flowSource);
                        matcher.find();
                        String flowId = matcher.group(1).trim();
                        isAddition = fetchSingleFlow(kestraClient, tenantId, flowSourceByNamespace.getKey(), flowId) == null;
                        flowWithSource = FlowWithSource.builder().source(flowSource).id(flowId).build();
                    } else {
                        String flowId = YamlParser.parse(flowSource, io.kestra.core.models.flows.Flow.class).getId();
                        kestraClient.flows().importFlows(false, tenantId, toNamedTempFile(flowId + ".yaml", flowSource.stripTrailing()));
                        // Determine addition vs update by checking if revision == 1
                        var imported = fetchSingleFlow(kestraClient, tenantId, flowSourceByNamespace.getKey(), flowId);
                        isAddition = imported != null && imported.getRevision() != null && imported.getRevision() == 1;
                        flowWithSource = imported != null ? imported : FlowWithSource.builder().source(flowSource).id(flowId).build();
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

            fetchFlowsFromKestra(kestraClient, tenantId, namespace).stream()
                .filter(flow -> !flowIdsImported.contains(flow.getId()))
                .forEach(flow ->
                {
                    if (!dryRun) {
                        try {
                            kestraClient.flows().deleteFlow(flow.getNamespace(), flow.getId(), tenantId);
                        } catch (ApiException e) {
                            throw new KestraRuntimeException("Failed to delete flow " + flow.getId(), e);
                        }
                    }
                    logDeletion(logger, "/_flows/" + flow.getId() + ".yml");
                });
        }

        Map<String, String> gitContentByFilePath;
        try (Stream<Path> walk = Files.walk(absoluteGitDirPath)) {
            List<Path> list = walk
                .filter(path ->
                {
                    String pathStr = path.toString();
                    return !pathStr.equals(absoluteGitDirPath.toString()) &&
                        !pathStr.contains("/.git/") && !pathStr.endsWith("/.git") &&
                        !pathStr.contains("/" + FLOWS_DIRECTORY + "/") && !pathStr.endsWith("/" + FLOWS_DIRECTORY) &&
                        !kestraIgnore.isIgnoredFile(pathStr, true);
                })
                .toList();
            gitContentByFilePath = list.stream()
                .map(Path::toFile)
                .collect(HashMap::new, throwBiConsumer((map, file) ->
                {
                    String relativePath = absoluteGitDirPath.relativize(file.toPath()).toString();
                    map.put(
                        "/" + (file.isDirectory() ? relativePath + "/" : relativePath),
                        file.isDirectory() ? null : FileUtils.readFileToString(file, "UTF-8")
                    );
                }), HashMap::putAll);
        }

        StorageInterface storage = ((DefaultRunContext) runContext).services().additionalService(StorageInterface.class);
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
            .collect(
                Collectors.toMap(
                    uri -> "/" + finalNamespaceFilePrefix.relativize(uri),
                    Function.identity()
                )
            );

        logger
            .info("Dry run is {}, {}performing following actions (- for deletions, + for creations, ~ for update or no modification):", dryRun ? "enabled" : "disabled", dryRun ? "not " : "");
        // perform all required deletions before-hand
        fullUriByRelativeNsFilesPath.forEach(throwBiConsumer((relativeNsFilePath, uri) ->
        {
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
            .forEach(throwConsumer(contentByFilePath ->
            {
                String path = contentByFilePath.getKey();
                if (fullUriByRelativeNsFilesPath.containsKey(path)) {
                    logUpdate(logger, path);
                } else {
                    logAddition(logger, path);
                }

                if (!dryRun) {
                    URI fileUri = finalNamespaceFilePrefix.resolve(path.replace("\\", "/").substring(1));
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

    private List<FlowWithSource> fetchFlowsFromKestra(KestraClient kestraClient, String tenantId, String namespace) {
        try {
            byte[] zippedFlows = kestraClient.flows().exportFlowsByQuery(
                tenantId,
                List.of(new QueryFilter().field(QueryFilterField.NAMESPACE).operation(QueryFilterOp.EQUALS).value(namespace))
            );
            List<FlowWithSource> flows = new ArrayList<>();
            try (
                var bais = new ByteArrayInputStream(zippedFlows);
                var zis = new ZipInputStream(bais)
            ) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (!entry.getName().endsWith(".yml") && !entry.getName().endsWith(".yaml")) {
                        continue;
                    }
                    var yaml = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
                    try {
                        flows.add(FlowWithSource.of(YamlParser.parse(yaml, io.kestra.core.models.flows.Flow.class), yaml));
                    } catch (Exception ignored) {
                    }
                }
            }
            return flows;
        } catch (IOException | ApiException e) {
            throw new KestraRuntimeException("Failed to export flows from Kestra for namespace " + namespace, e);
        }
    }

    private FlowWithSource fetchSingleFlow(KestraClient kestraClient, String tenantId, String namespace, String flowId) {
        try {
            byte[] zippedFlows = kestraClient.flows().exportFlowsByQuery(
                tenantId,
                List.of(
                    new QueryFilter().field(QueryFilterField.NAMESPACE).operation(QueryFilterOp.EQUALS).value(namespace),
                    new QueryFilter().field(QueryFilterField.ID).operation(QueryFilterOp.EQUALS).value(flowId)
                )
            );
            try (
                var bais = new ByteArrayInputStream(zippedFlows);
                var zis = new ZipInputStream(bais)
            ) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (!entry.getName().endsWith(".yml") && !entry.getName().endsWith(".yaml")) {
                        continue;
                    }
                    var yaml = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
                    return FlowWithSource.of(YamlParser.parse(yaml, io.kestra.core.models.flows.Flow.class), yaml);
                }
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private File toNamedTempFile(String fileName, String yaml) {
        try {
            Path tmpPath = Files.createTempDirectory("kestra-import")
                .resolve(fileName.endsWith(".yaml") ? fileName : fileName + ".yaml");
            Files.createDirectories(tmpPath.getParent());
            Files.writeString(tmpPath, yaml, StandardCharsets.UTF_8);
            return tmpPath.toFile();
        } catch (IOException e) {
            throw new KestraRuntimeException("Failed to create named file for: " + fileName, e);
        }
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
