package io.kestra.plugin.git;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevTag;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.RefSpec;
import org.slf4j.Logger;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractCloningTask extends AbstractGitTask {
    @Schema(
        title = "Clone submodules",
        description = "Default false; enable to fetch and initialize nested submodules."
    )
    protected Property<Boolean> cloneSubmodules;

    protected void checkoutCommit(Git git, String sha, Logger logger) throws Exception {
        // Ensure we have a full history in case the repo was shallow by default on the remote
        // or if the requested SHA is deep in history.
        try {
            // Fetch all branches and tags to guarantee the target commit is available locally.
            git.fetch()
                .setRefSpecs(
                    new RefSpec("+refs/heads/*:refs/remotes/origin/*"),
                    new RefSpec("+refs/tags/*:refs/tags/*")
                )
                .call();
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

    protected void checkoutTag(Git git, String rTagName, Logger logger) throws Exception {
        git.fetch()
            .setRefSpecs(new RefSpec("+refs/tags/*:refs/tags/*"))
            .call();

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
