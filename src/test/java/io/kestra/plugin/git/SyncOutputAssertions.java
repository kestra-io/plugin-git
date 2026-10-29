package io.kestra.plugin.git;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import io.kestra.plugin.git.shared.AbstractSyncTask;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

final class SyncOutputAssertions {
    private SyncOutputAssertions() {
    }

    static void assertSyncedBranchTip(AbstractSyncTask.Output output, String repositoryUrl, String pat, String branch) throws Exception {
        var tip = Git.lsRemoteRepository()
            .setRemote(repositoryUrl)
            .setCredentialsProvider(new UsernamePasswordCredentialsProvider(pat, pat))
            .setHeads(true)
            .callAsMap()
            .get("refs/heads/" + branch)
            .getObjectId()
            .getName();

        assertThat(output.getCommitId(), is(tip));
        assertThat(output.getCommitURL(), is(repositoryUrl.replaceFirst("\\.git$", "") + "/commit/" + tip));
    }
}
