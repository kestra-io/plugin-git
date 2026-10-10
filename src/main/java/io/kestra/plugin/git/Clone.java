package io.kestra.plugin.git;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.TagOpt;

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
        description = """
            When true, caches the .git directory in Kestra internal storage and restores it on subsequent runs, \
            fetching only new commits instead of performing a full clone. Note that only metadata and objects inside \
            .git are cached; working tree files outside .git are not persisted in the cache. Healing corrupted or \
            degraded object caches during incremental fetches is best-effort. Defaults to false.\
            """
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> cache = Property.ofValue(false);

    @Schema(
        title = "Cache TTL (Time To Live)",
        description = """
            After this duration, the repository cache will be invalidated. Defaults to no expiration.\
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> cacheTtl;

    @Override
    public Clone.Output run(RunContext runContext) throws Exception {
        var rUrl = runContext.render(this.url).as(String.class).orElse(null);
        var cloneOptions = resolveCloneOptions(runContext);

        var path = runContext.workingDir().path();
        if (this.directory != null) {
            var rDirectory = runContext.render(this.directory).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("directory rendered to an empty value - set a path or remove the property"));
            path = runContext.workingDir().resolve(Path.of(rDirectory));
        }

        configureHttpTransport(runContext);

        // we add this method to configure ssl to allow self signed certs
        configureEnvironmentWithSsl(runContext);

        var rDepth = (this.commit == null && this.tag == null) ? runContext.render(this.depth).as(Integer.class).orElse(1) : null;
        var rCache = this.cache != null && runContext.render(this.cache).as(Boolean.class).orElse(false);
        var rCacheTtl = this.cacheTtl != null ? runContext.render(this.cacheTtl).as(Duration.class).orElse(null) : null;

        if (rCache && rUrl != null && !stripUserInfo(rUrl).equals(rUrl)) {
            runContext.logger().warn(
                "Caching is enabled for repository URL containing embedded credentials; sensitive user info will be stripped from Git configuration before caching."
            );
        }

        var cacheKey = "git-cache";
        var objectId = computeCacheObjectId(rUrl, cloneOptions.branch());
        var gitDir = path.resolve(".git");
        var cacheRestored = false;

        if (rCache && !isGitRepository(path)) {
            cacheRestored = restoreCache(runContext, rUrl, path, gitDir, cacheKey, objectId, rCacheTtl);
        }

        var headBefore = resolveHead(path);
        var objectsBefore = ObjectsFingerprint.of(gitDir);

        var resultDirectory = "";
        var repositoryUpdated = false;

        if (isGitRepository(path)) {
            try {
                var updateResult = updateRepository(runContext, path, rUrl, cloneOptions, rDepth);
                resultDirectory = updateResult.directory();
                repositoryUpdated = updateResult.updated();
            } catch (Exception e) {
                if (cacheRestored) {
                    runContext.logger().warn("Failed to update cached repository for '{}', falling back to normal clone", stripUserInfo(rUrl), e);
                    discardCache(runContext, gitDir, cacheKey, objectId);
                    cacheRestored = false;
                    var result = cloneFresh(runContext, rUrl, path, cloneOptions, rDepth);
                    resultDirectory = result.directory();
                } else {
                    throw e;
                }
            }
        } else {
            var result = cloneFresh(runContext, rUrl, path, cloneOptions, rDepth);
            resultDirectory = result.directory();
        }

        var headAfter = resolveHead(path);
        var objectsAfter = ObjectsFingerprint.of(gitDir);
        var objectsUpdated = objectsBefore.hasChanged(objectsAfter);

        if (rCache) {
            updateCache(
                runContext,
                path,
                gitDir,
                rUrl,
                cacheKey,
                objectId,
                cacheRestored,
                headBefore,
                headAfter,
                repositoryUpdated,
                objectsUpdated
            );
        }

        return Output.builder().directory(resultDirectory).build();
    }

    private CloneService.CloneResult cloneFresh(
        RunContext runContext,
        String rUrl,
        Path path,
        CloneOptions cloneOptions,
        Integer rDepth) throws Exception {
        var rCommit = this.commit != null
            ? runContext.render(this.commit).as(String.class).orElseThrow(() -> new IllegalArgumentException("commit rendered to an empty value - set a SHA or remove the property"))
            : null;
        var rTag = this.tag != null
            ? runContext.render(this.tag).as(String.class).orElseThrow(() -> new IllegalArgumentException("tag rendered to an empty value - set a tag name or remove the property"))
            : null;

        return CloneService.clone(
            runContext, this, CloneService.CloneRequest.builder()
                .url(rUrl)
                .path(path)
                .branch(cloneOptions.branch())
                .depth(rDepth)
                .commit(rCommit)
                .tag(rTag)
                .cloneAllBranches(cloneOptions.cloneAllBranches())
                .noTags(cloneOptions.noTags())
                .cloneSubmodules(this.cloneSubmodules)
                .build()
        );
    }

    private boolean restoreCache(
        RunContext runContext,
        String rUrl,
        Path path,
        Path gitDir,
        String cacheKey,
        String objectId,
        Duration rCacheTtl) {
        try {
            var maybeCacheFile = runContext.storage().getCacheFile(cacheKey, objectId, rCacheTtl);
            if (maybeCacheFile.isPresent()) {
                runContext.logger().info("Found cached repository for '{}', restoring cache...", stripUserInfo(rUrl));
                Files.createDirectories(gitDir);
                var count = 0;
                try (var is = maybeCacheFile.get()) {
                    count = extractZipArchive(is, gitDir);
                }
                if (count == 0) {
                    throw new IOException("Cache archive contained no entries or is invalid");
                }
                if (!isValidGitRepository(path)) {
                    throw new IOException("Restored repository is invalid or corrupt");
                }
                return true;
            }
        } catch (Exception e) {
            runContext.logger().warn("Failed to restore repository cache for '{}', proceeding with normal clone", stripUserInfo(rUrl), e);
            discardCache(runContext, gitDir, cacheKey, objectId);
        }
        return false;
    }

    private void updateCache(
        RunContext runContext,
        Path path,
        Path gitDir,
        String rUrl,
        String cacheKey,
        String objectId,
        boolean cacheRestored,
        ObjectId headBefore,
        ObjectId headAfter,
        boolean repositoryUpdated,
        boolean objectsUpdated) {
        var shouldUpdateCache = !cacheRestored
            || headBefore == null
            || !Objects.equals(headBefore, headAfter)
            || repositoryUpdated
            || objectsUpdated;

        if (shouldUpdateCache && Files.exists(gitDir)) {
            try {
                var originalUrl = sanitizeGitConfigBeforeArchiving(path, runContext);
                try {
                    var tempZip = runContext.workingDir().createTempFile(".zip");
                    createZipArchive(gitDir, tempZip);
                    runContext.storage().putCacheFile(tempZip.toFile(), cacheKey, objectId);
                    runContext.logger().info("Updated repository cache for '{}'", stripUserInfo(rUrl));
                } finally {
                    if (originalUrl != null) {
                        restoreGitConfigOriginUrl(path, originalUrl, runContext);
                    }
                }
            } catch (Exception e) {
                runContext.logger().warn("Failed to update repository cache for '{}'", stripUserInfo(rUrl), e);
            }
        } else if (cacheRestored) {
            runContext.logger().info("Repository cache is already up-to-date for '{}', skipping cache upload", stripUserInfo(rUrl));
        }
    }

    private void discardCache(RunContext runContext, Path gitDir, String cacheKey, String objectId) {
        var logger = runContext.logger();
        try {
            if (Files.exists(gitDir)) {
                FileUtils.deleteDirectory(gitDir.toFile());
            }
        } catch (Exception e) {
            logger.warn("Failed to delete local .git directory at '{}'", gitDir, e);
        }
        try {
            runContext.storage().deleteCacheFile(cacheKey, objectId);
        } catch (Exception e) {
            logger.warn("Failed to delete cache file '{}' for object '{}' in storage", cacheKey, objectId, e);
        }
    }

    static String sanitizeGitConfigBeforeArchiving(Path path, RunContext runContext) {
        try (var git = Git.open(path.toFile())) {
            var config = git.getRepository().getConfig();
            var originUrl = config.getString("remote", "origin", "url");
            if (originUrl != null) {
                var sanitized = stripUserInfo(originUrl);
                if (!sanitized.equals(originUrl)) {
                    runContext.logger().warn("Sanitizing repository configuration before caching: credentials in remote origin URL stripped");
                    config.setString("remote", "origin", "url", sanitized);
                    config.save();
                    return originUrl;
                }
            }
        } catch (Exception e) {
            runContext.logger().debug("Could not sanitize git config before archiving", e);
        }
        return null;
    }

    static void restoreGitConfigOriginUrl(Path path, String originalUrl, RunContext runContext) {
        try (var git = Git.open(path.toFile())) {
            var config = git.getRepository().getConfig();
            config.setString("remote", "origin", "url", originalUrl);
            config.save();
        } catch (Exception e) {
            runContext.logger().debug("Could not restore git config origin URL after archiving", e);
        }
    }

    static String stripUserInfo(String url) {
        if (url == null) {
            return null;
        }
        try {
            var uri = URI.create(url);
            var scheme = uri.getScheme();
            if (scheme != null && (scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https")) && uri.getUserInfo() != null) {
                return new URI(
                    uri.getScheme(),
                    null,
                    uri.getHost(),
                    uri.getPort(),
                    uri.getPath(),
                    uri.getQuery(),
                    uri.getFragment()
                ).toString();
            }
        } catch (Exception ignored) {
        }
        return url;
    }

    private UpdateResult updateRepository(
        RunContext runContext,
        Path path,
        String rUrl,
        CloneOptions cloneOptions,
        Integer rDepth) throws Exception {
        var logger = runContext.logger();
        logger.info("Existing Git repository found at '{}', fetching latest changes from '{}'", path, stripUserInfo(rUrl));

        var hasCommit = this.commit != null;
        var hasTag = this.tag != null;
        var updated = false;

        try (var git = Git.open(path.toFile())) {
            var config = git.getRepository().getConfig();
            var existingUrl = config.getString("remote", "origin", "url");
            if (existingUrl == null || !existingUrl.equals(rUrl)) {
                config.setString("remote", "origin", "url", rUrl);
                config.save();
            }

            applyGitConfig(git.getRepository(), runContext);

            var refSpecs = new ArrayList<RefSpec>();
            if (!cloneOptions.cloneAllBranches() && cloneOptions.branch() != null) {
                var branchName = shortBranchName(cloneOptions.branch());
                refSpecs.add(new RefSpec("+refs/heads/" + branchName + ":refs/remotes/origin/" + branchName));
            } else {
                refSpecs.add(new RefSpec("+refs/heads/*:refs/remotes/origin/*"));
            }

            if (!cloneOptions.noTags()) {
                refSpecs.add(new RefSpec("+refs/tags/*:refs/tags/*"));
            }

            var fetchCommand = git.fetch()
                .setRemote("origin")
                .setRefSpecs(refSpecs);

            if (cloneOptions.noTags()) {
                fetchCommand.setTagOpt(TagOpt.NO_TAGS);
            }

            if (!hasCommit && !hasTag && rDepth != null) {
                fetchCommand.setDepth(rDepth);
            }

            var fetchResult = authentified(fetchCommand, runContext).call();
            if (fetchResult != null && fetchResult.getTrackingRefUpdates().stream()
                .anyMatch(u -> u.getResult() != RefUpdate.Result.NO_CHANGE)) {
                updated = true;
            }

            if (hasCommit) {
                var rSha = runContext.render(this.commit).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("commit rendered to an empty value - set a SHA or remove the property"));
                var target = git.getRepository().resolve(rSha);
                if (isShallowRepository(git.getRepository()) && (target == null || !git.getRepository().getObjectDatabase().has(target))) {
                    logger.info("Commit '{}' not found in shallow repository, unshallowing repository...", rSha);
                    var unshallow = git.fetch()
                        .setRemote("origin")
                        .setRefSpecs(refSpecs)
                        .setUnshallow(true);
                    if (cloneOptions.noTags()) {
                        unshallow.setTagOpt(TagOpt.NO_TAGS);
                    }
                    authentified(unshallow, runContext).call();
                    updated = true;
                }
                CloneService.checkoutCommit(git, rSha, logger, cloneOptions.noTags());
                git.reset().setMode(ResetType.HARD).call();
            } else if (hasTag) {
                var rTagName = runContext.render(this.tag).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("tag rendered to an empty value - set a tag name or remove the property"));
                var tagTarget = git.getRepository().resolve("refs/tags/" + rTagName);
                if (tagTarget == null) {
                    tagTarget = git.getRepository().resolve(rTagName);
                }
                if (isShallowRepository(git.getRepository()) && (tagTarget == null || !git.getRepository().getObjectDatabase().has(tagTarget))) {
                    logger.info("Tag '{}' not found in shallow repository, unshallowing repository...", rTagName);
                    var unshallow = git.fetch()
                        .setRemote("origin")
                        .setRefSpecs(refSpecs)
                        .setUnshallow(true);
                    if (cloneOptions.noTags()) {
                        unshallow.setTagOpt(TagOpt.NO_TAGS);
                    }
                    authentified(unshallow, runContext).call();
                    updated = true;
                }
                CloneService.checkoutTag(git, rTagName, logger, cloneOptions.noTags());
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

                var cleanBranch = shortBranchName(targetBranch);
                var remoteBranch = "origin/" + cleanBranch;
                var localBranchExists = git.getRepository().exactRef("refs/heads/" + cleanBranch) != null;

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

            return new UpdateResult(git.getRepository().getDirectory().getParent(), updated);
        }
    }

    private record UpdateResult(String directory, boolean updated) {
    }

    private record ObjectsFingerprint(Set<String> packFiles, long looseObjectsCount) {
        static ObjectsFingerprint of(Path gitDir) {
            if (!Files.isDirectory(gitDir)) {
                return new ObjectsFingerprint(Collections.emptySet(), 0);
            }
            var objectsDir = gitDir.resolve("objects");
            if (!Files.isDirectory(objectsDir)) {
                return new ObjectsFingerprint(Collections.emptySet(), 0);
            }
            var packDir = objectsDir.resolve("pack");
            var packFiles = new HashSet<String>();
            if (Files.isDirectory(packDir)) {
                try (var stream = Files.list(packDir)) {
                    stream.filter(p -> p.getFileName().toString().endsWith(".pack"))
                        .map(p -> p.getFileName().toString())
                        .forEach(packFiles::add);
                } catch (IOException ignored) {
                }
            }
            var looseCount = 0L;
            try (var stream = Files.list(objectsDir)) {
                looseCount = stream
                    .filter(p -> Files.isDirectory(p) && !p.getFileName().toString().equals("pack") && !p.getFileName().toString().equals("info"))
                    .mapToLong(p ->
                    {
                        try (var sub = Files.list(p)) {
                            return sub.count();
                        } catch (IOException e) {
                            return 0L;
                        }
                    })
                    .sum();
            } catch (IOException ignored) {
            }
            return new ObjectsFingerprint(packFiles, looseCount);
        }

        boolean hasChanged(ObjectsFingerprint after) {
            if (after == null) {
                return false;
            }
            return !this.packFiles.equals(after.packFiles) || this.looseObjectsCount != after.looseObjectsCount;
        }
    }

    private static boolean isGitRepository(Path path) {
        if (!Files.exists(path)) {
            return false;
        }
        var gitDir = path.resolve(".git").toFile();
        return gitDir.exists() && (gitDir.isDirectory() || gitDir.isFile());
    }

    private static boolean isValidGitRepository(Path path) {
        if (!isGitRepository(path)) {
            return false;
        }
        try (var git = Git.open(path.toFile())) {
            return git.getRepository().resolve("HEAD") != null;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isShallowRepository(Repository repo) {
        try {
            return Files.exists(repo.getDirectory().toPath().resolve("shallow"))
                || !repo.getObjectDatabase().getShallowCommits().isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    private static ObjectId resolveHead(Path path) {
        if (!isGitRepository(path)) {
            return null;
        }
        try (var git = Git.open(path.toFile())) {
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
        var sanitizedUrl = stripUserInfo(url);
        var key = sanitizedUrl + (branch != null && !branch.isBlank() ? ":" + shortBranchName(branch) : "");
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            var hash = digest.digest(key.getBytes(StandardCharsets.UTF_8));
            var hexString = new StringBuilder();
            for (byte b : hash) {
                var hex = Integer.toHexString(0xff & b);
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

    static void createZipArchive(Path gitDir, Path zipFile) throws IOException {
        try (var zos = new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(zipFile)))) {
            Files.walkFileTree(gitDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (attrs.isSymbolicLink()) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    if (!gitDir.equals(dir)) {
                        var entryName = gitDir.relativize(dir).toString().replace('\\', '/') + "/";
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
                    var entryName = gitDir.relativize(file).toString().replace('\\', '/');
                    var entry = new ZipEntry(entryName);
                    entry.setTime(attrs.lastModifiedTime().toMillis());
                    zos.putNextEntry(entry);
                    Files.copy(file, zos);
                    zos.closeEntry();
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    static int extractZipArchive(InputStream is, Path targetDir) throws IOException {
        var count = 0;
        try (var zis = new ZipInputStream(new BufferedInputStream(is))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                var targetPath = targetDir.resolve(entry.getName()).normalize();
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
                count++;
            }
        }
        return count;
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
