package io.kestra.plugin.git;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.FetchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.TagOpt;
import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.git.shared.AbstractCloningTask;
import io.kestra.plugin.git.shared.services.CloneService;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Clone a Git repository",
    description = "Clones a repository over HTTP(S) or SSH, optionally checking out a branch, tag, or commit. Defaults to a shallow clone (depth 1) unless a tag or commit is requested; set `cloneSubmodules` to fetch submodules. Supports repository caching to avoid full re-clones across executions."
)
@Plugin(
    examples = {
        @Example(
            title = "Clone a public GitHub repository.",
            full = true,
            code = """
                id: git_clone
                namespace: company.team

                tasks:
                  - id: wdir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/blueprints
                        branch: main
                """
        ),
        @Example(
            title = "Clone a repository with caching enabled to avoid full re-clones across executions.",
            full = true,
            code = """
                id: git_clone_cached
                namespace: company.team

                tasks:
                  - id: clone
                    type: io.kestra.plugin.git.Clone
                    url: https://github.com/kestra-io/blueprints
                    branch: main
                    cache: true
                    cacheTtl: PT24H
                """
        ),
        @Example(
            title = "Clone a private repository from an HTTP server such as a private GitHub repository using a [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token).",
            full = true,
            code = """
                id: git_clone
                namespace: company.team

                tasks:
                  - id: wdir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/blueprints
                        branch: main
                        username: git_username
                        password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                """
        ),
        @Example(
            title = "Clone a repository from an SSH server. If you want to clone the repository into a specific directory, you can configure the `directory` property as shown below.",
            full = true,
            code = """
                id: git_clone
                namespace: company.team

                tasks:
                  - id: wdir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone
                        type: io.kestra.plugin.git.Clone
                        url: git@github.com:kestra-io/kestra.git
                        directory: kestra
                        privateKey: "{{ secret('SSH_PRIVATE_KEY') }}"
                        passphrase: "{{ secret('SSH_PASSPHRASE') }}"
                """
        ),
        @Example(
            title = "Clone a GitHub repository and run a Python ETL script. Note that the `WorkingDirectory` task is required so that the Python script shares the same local file system with files cloned from GitHub in the previous task.",
            full = true,
            code = """
                id: git_python
                namespace: company.team

                tasks:
                  - id: file_system
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone_repository
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/examples
                        branch: main
                      - id: python_etl
                        type: io.kestra.plugin.scripts.python.Commands
                        dependencies:
                          - requests
                          - pandas
                        commands:
                          - python examples/scripts/etl_script.py
                """
        ),
        @Example(
            title = "Clone then checkout a specific commit (detached HEAD).",
            full = true,
            code = """
                id: git_clone_commit
                namespace: company.team

                tasks:
                  - id: clone_at_sha
                    type: io.kestra.plugin.git.Clone
                    url: https://github.com/kestra-io/kestra
                    commit: 98189392a2a4ea0b1a951cd9dbbfe72f0193d77b
                """
        ),
    }
)
public class Clone extends AbstractCloningTask implements RunnableTask<Clone.Output> {
    @Schema(
        title = "Target directory",
        description = "Subdirectory under the working directory where the repo is cloned; defaults to the working directory root."
    )
    @PluginProperty(group = "destination")
    private Property<String> directory;

    @Schema(
        title = "Branch to checkout",
        description = "Used only when no commit or tag is specified."
    )
    @PluginProperty(group = "advanced")
    private Property<String> branch;

    @Schema(
        title = "Shallow clone depth",
        description = "Defaults to 1. Ignored when `commit` or `tag` is set to ensure history is available."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> depth = Property.ofValue(1);

    @Schema(
        title = "Commit SHA to checkout",
        description = "Detached HEAD checkout; short SHA allowed. Overrides `branch` and disables shallow clone."
    )
    @PluginProperty(group = "advanced")
    private Property<String> commit;

    @Schema(
        title = "Tag to checkout",
        description = "Ignored when `commit` is set; performs a full fetch to reach the tag."
    )
    @PluginProperty(group = "advanced")
    private Property<String> tag;

    @Schema(
        title = "Clone all branches",
        description = "When true, clones all remote branches. When false, follows single-branch clone behavior."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> cloneAllBranches = Property.ofValue(true);

    @Schema(
        title = "Do not fetch tags",
        description = "When true, skip fetching tags during clone and fetch fallback."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> noTags = Property.ofValue(false);

    @Schema(
        title = "Enable repository caching",
        description = "When true, caches the cloned repository in Kestra internal storage and restores it on subsequent runs, fetching only new commits instead of performing a full clone."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> cache = Property.ofValue(false);

    @Schema(
        title = "Cache TTL (Time To Live)",
        description = "After this duration, the cache will be invalidated. Defaults to no expiration."
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> cacheTtl;

    @Override
    public Clone.Output run(RunContext runContext) throws Exception {

        String url = runContext.render(this.url).as(String.class).orElse(null);
        var cloneOptions = resolveCloneOptions(runContext);

        Path path = runContext.workingDir().path();
        if (this.directory != null) {
            String directory = runContext.render(this.directory).as(String.class).orElseThrow();
            path = runContext.workingDir().resolve(Path.of(directory));
        }

        configureHttpTransport(runContext);

        // we add this method to configure ssl to allow self signed certs
        configureEnvironmentWithSsl(runContext);

        var rDepth = (this.commit == null && this.tag == null) ? runContext.render(this.depth).as(Integer.class).orElse(1) : null;
        var rCache = this.cache != null && runContext.render(this.cache).as(Boolean.class).orElse(false);
        var rCacheTtl = this.cacheTtl != null ? runContext.render(this.cacheTtl).as(Duration.class).orElse(null) : null;

        String cacheKey = "git-cache";
        String objectId = computeCacheObjectId(url, cloneOptions.branch());
        Path gitDir = path.resolve(".git");
        boolean cacheRestored = false;

        if (rCache && !isGitRepository(path)) {
            try {
                var maybeCacheFile = runContext.storage().getCacheFile(cacheKey, objectId, rCacheTtl);
                if (maybeCacheFile.isPresent()) {
                    runContext.logger().info("Found cached repository for '{}', restoring cache...", url);
                    Files.createDirectories(gitDir);
                    try (InputStream is = maybeCacheFile.get()) {
                        extractZipArchive(is, gitDir);
                    }
                    cacheRestored = isGitRepository(path);
                }
            } catch (Exception e) {
                runContext.logger().warn("Failed to restore repository cache for '{}', proceeding with normal clone", url, e);
                try {
                    if (Files.exists(gitDir)) {
                        FileUtils.deleteDirectory(gitDir.toFile());
                    }
                } catch (Exception ignored) {
                }
            }
        }

        ObjectId headBefore = resolveHead(path);

        String resultDirectory;
        if (isGitRepository(path)) {
            resultDirectory = updateRepository(runContext, path, url, cloneOptions, rDepth);
        } else {
            // CloneService.clone() already logs the start and any transport failure, so both editions share a single log line.
            var result = CloneService.clone(
                runContext, this, CloneService.CloneRequest.builder()
                    .url(url)
                    .path(path)
                    .branch(cloneOptions.branch())
                    .depth(rDepth)
                    .commit(this.commit != null ? runContext.render(this.commit).as(String.class).orElseThrow() : null)
                    .tag(this.tag != null ? runContext.render(this.tag).as(String.class).orElseThrow() : null)
                    .cloneAllBranches(cloneOptions.cloneAllBranches())
                    .noTags(cloneOptions.noTags())
                    .cloneSubmodules(this.cloneSubmodules)
                    .build()
            );
            resultDirectory = result.directory();
        }

        ObjectId headAfter = resolveHead(path);

        if (rCache) {
            boolean shouldUpdateCache = !cacheRestored || headBefore == null || !Objects.equals(headBefore, headAfter);
            if (shouldUpdateCache && Files.exists(gitDir)) {
                try {
                    Path tempZip = runContext.workingDir().createTempFile(".zip");
                    createZipArchive(gitDir, tempZip);
                    runContext.storage().putCacheFile(tempZip.toFile(), cacheKey, objectId);
                    runContext.logger().info("Updated repository cache for '{}'", url);
                } catch (Exception e) {
                    runContext.logger().warn("Failed to update repository cache for '{}'", url, e);
                }
            } else if (cacheRestored) {
                runContext.logger().info("Repository cache is already up-to-date for '{}', skipping cache upload", url);
            }
        }

        return Output.builder().directory(resultDirectory).build();
    }

    private String updateRepository(
        RunContext runContext,
        Path path,
        String url,
        CloneOptions cloneOptions,
        Integer depth) throws Exception {
        Logger logger = runContext.logger();
        logger.info("Existing Git repository found at '{}', fetching latest changes from '{}'", path, url);

        boolean hasCommit = this.commit != null;
        boolean hasTag = this.tag != null;

        try (Git git = Git.open(path.toFile())) {
            StoredConfig config = git.getRepository().getConfig();
            String existingUrl = config.getString("remote", "origin", "url");
            if (existingUrl == null || !existingUrl.equals(url)) {
                config.setString("remote", "origin", "url", url);
                config.save();
            }

            applyGitConfig(git.getRepository(), runContext);

            List<RefSpec> refSpecs = new ArrayList<>();
            if (!cloneOptions.cloneAllBranches() && cloneOptions.branch() != null) {
                String branchName = shortBranchName(cloneOptions.branch());
                refSpecs.add(new RefSpec("+refs/heads/" + branchName + ":refs/remotes/origin/" + branchName));
            } else {
                refSpecs.add(new RefSpec("+refs/heads/*:refs/remotes/origin/*"));
            }

            if (!cloneOptions.noTags()) {
                refSpecs.add(new RefSpec("+refs/tags/*:refs/tags/*"));
            }

            FetchCommand fetchCommand = git.fetch()
                .setRemote("origin")
                .setRefSpecs(refSpecs);

            if (cloneOptions.noTags()) {
                fetchCommand.setTagOpt(TagOpt.NO_TAGS);
            }

            if (!hasCommit && !hasTag && depth != null) {
                fetchCommand.setDepth(depth);
            }

            authentified(fetchCommand, runContext).call();

            if (hasCommit) {
                String sha = runContext.render(this.commit).as(String.class).orElseThrow();
                CloneService.checkoutCommit(git, sha, logger, cloneOptions.noTags());
                git.reset().setMode(ResetType.HARD).call();
            } else if (hasTag) {
                String tagName = runContext.render(this.tag).as(String.class).orElseThrow();
                CloneService.checkoutTag(git, tagName, logger, cloneOptions.noTags());
                git.reset().setMode(ResetType.HARD).call();
            } else {
                var targetBranch = cloneOptions.branch();
                if (targetBranch == null) {
                    var headRef = git.getRepository().exactRef("refs/remotes/origin/HEAD");
                    if (headRef != null && headRef.getTarget() != null) {
                        targetBranch = headRef.getTarget().getName().replace("refs/remotes/origin/", "");
                    }
                }

                if (targetBranch == null) {
                    if (git.getRepository().exactRef("refs/remotes/origin/main") != null) {
                        targetBranch = "main";
                    } else if (git.getRepository().exactRef("refs/remotes/origin/master") != null) {
                        targetBranch = "master";
                    } else {
                        throw new IllegalStateException(
                            "Cannot determine the default branch. Please specify the 'branch' property explicitly."
                        );
                    }
                }

                String cleanBranch = shortBranchName(targetBranch);
                String remoteBranch = "origin/" + cleanBranch;
                boolean localBranchExists = git.getRepository().exactRef("refs/heads/" + cleanBranch) != null;

                if (!localBranchExists) {
                    git.checkout()
                        .setName(cleanBranch)
                        .setCreateBranch(true)
                        .setStartPoint(remoteBranch)
                        .call();

                    git.reset()
                        .setMode(ResetType.HARD)
                        .setRef(remoteBranch)
                        .call();
                } else {
                    git.checkout()
                        .setName(cleanBranch)
                        .call();

                    git.reset()
                        .setMode(ResetType.HARD)
                        .setRef(remoteBranch)
                        .call();
                }

                logger.info("Checked out and updated branch {} from {}", cleanBranch, remoteBranch);
            }

            if (this.cloneSubmodules != null && runContext.render(this.cloneSubmodules).as(Boolean.class).orElse(false)) {
                git.submoduleInit().call();
                authentified(git.submoduleUpdate(), runContext).call();
            }

            return git.getRepository().getDirectory().getParent();
        }
    }

    private static boolean isGitRepository(Path path) {
        if (!Files.exists(path)) {
            return false;
        }
        File gitDir = path.resolve(".git").toFile();
        return gitDir.exists() && (gitDir.isDirectory() || gitDir.isFile());
    }

    private static ObjectId resolveHead(Path path) {
        if (!isGitRepository(path)) {
            return null;
        }
        try (Git git = Git.open(path.toFile())) {
            return git.getRepository().resolve("HEAD");
        } catch (Exception e) {
            return null;
        }
    }

    private static String shortBranchName(String branch) {
        if (branch == null) {
            return null;
        }
        return branch.startsWith("refs/heads/") ? branch.substring("refs/heads/".length()) : branch;
    }

    static String computeCacheObjectId(String url, String branch) {
        String key = url + (branch != null && !branch.isBlank() ? ":" + shortBranchName(branch) : "");
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(key.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            return Integer.toHexString(key.hashCode());
        }
    }

    private static void createZipArchive(Path gitDir, Path zipFile) throws IOException {
        try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(zipFile)))) {
            Files.walkFileTree(gitDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (attrs.isSymbolicLink()) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    if (!gitDir.equals(dir)) {
                        String entryName = gitDir.relativize(dir).toString().replace('\\', '/') + "/";
                        zos.putNextEntry(new ZipEntry(entryName));
                        zos.closeEntry();
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (attrs.isSymbolicLink() || file.equals(zipFile)) {
                        return FileVisitResult.CONTINUE;
                    }
                    String entryName = gitDir.relativize(file).toString().replace('\\', '/');
                    ZipEntry entry = new ZipEntry(entryName);
                    entry.setTime(attrs.lastModifiedTime().toMillis());
                    zos.putNextEntry(entry);
                    Files.copy(file, zos);
                    zos.closeEntry();
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    private static void extractZipArchive(InputStream is, Path targetDir) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                Path targetPath = targetDir.resolve(entry.getName()).normalize();
                if (!targetPath.startsWith(targetDir)) {
                    throw new IOException("Zip entry escapes target directory: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(targetPath);
                } else {
                    Files.createDirectories(targetPath.getParent());
                    Files.copy(zis, targetPath, StandardCopyOption.REPLACE_EXISTING);
                }
                zis.closeEntry();
            }
        }
    }

    private CloneOptions resolveCloneOptions(RunContext runContext) throws Exception {
        var rBranch = runContext.render(this.branch).as(String.class).orElse(null);
        var rCloneAllBranches = runContext.render(this.cloneAllBranches).as(Boolean.class).orElse(true);
        var rNoTags = runContext.render(this.noTags).as(Boolean.class).orElse(false);

        if (!rCloneAllBranches) {
            if (rBranch == null || rBranch.isBlank()) {
                throw new IllegalArgumentException(
                    "Invalid clone configuration: when `cloneAllBranches` is false, `branch` must be set."
                );
            }
        }

        if (this.tag != null && rNoTags) {
            throw new IllegalArgumentException("Invalid clone configuration: `tag` cannot be used with `noTags: true`.");
        }

        return new CloneOptions(rBranch, rCloneAllBranches, rNoTags);
    }

    private record CloneOptions(
        String branch,
        boolean cloneAllBranches,
        boolean noTags) {
    }

    @Override
    @NotNull
    public Property<String> getUrl() {
        return super.getUrl();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Path where the repository is cloned"
        )
        private final String directory;
    }
}
