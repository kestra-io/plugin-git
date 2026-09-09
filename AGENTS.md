# Kestra Git Plugin

## What

- Provides plugin components under `io.kestra.plugin.git`.
- Includes classes such as `SyncFlow`, `Sync`, `SyncNamespaceFiles`, `PushNamespaceFiles`.

## Why

- What user problem does this solve? Teams need to clone, fetch, and interact with Git repositories from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Git steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Git.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `git`

Infrastructure dependencies (Docker Compose services):

- `gitea`
- `gitea_data`

### Key Plugin Classes

- `io.kestra.plugin.git.Clone`
- `io.kestra.plugin.git.NamespaceSync`
- `io.kestra.plugin.git.Push`
- `io.kestra.plugin.git.PushExecutionFiles`
- `io.kestra.plugin.git.PushFlows`
- `io.kestra.plugin.git.PushNamespaceFiles`
- `io.kestra.plugin.git.Sync`
- `io.kestra.plugin.git.SyncFlow`
- `io.kestra.plugin.git.SyncFlows`
- `io.kestra.plugin.git.SyncNamespaceFiles`
- `io.kestra.plugin.git.TenantSync`

### Project Structure

```
plugin-git/
├── src/main/java/io/kestra/plugin/git/
├── src/test/java/io/kestra/plugin/git/
├── build.gradle
└── README.md
```

### Shared kernel

The connection, authentication and clone/push/sync plumbing (`AbstractGitTask`, `AbstractKestraTask`,
`AbstractCloningTask`, `AbstractSyncTask`, `AbstractPushTask`, `KestraApiConnection`, `KestraApiAuth`, `GitService`,
`CloneService`, `SshTransportConfigCallback`) lives in `io.kestra.plugin.git.shared.*`, published from
[`plugin-git-lib`](https://github.com/kestra-io/plugin-git-lib) and consumed here via
`api 'io.kestra.plugin:plugin-git-lib:...'`. It is shared with `plugin-ee-git` (Enterprise Edition). **Do not
re-implement or fork this plumbing in this repository** — bump the lib version instead, and land the fix there so
both editions pick it up. Only concrete, registered tasks (`Clone`, `Push*`, `Sync*`, `NamespaceSync`, `TenantSync`)
belong in this repository.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
