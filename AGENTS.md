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
- `io.kestra.plugin.git.PushDashboards`
- `io.kestra.plugin.git.PushExecutionFiles`
- `io.kestra.plugin.git.PushFlows`
- `io.kestra.plugin.git.PushNamespaceFiles`
- `io.kestra.plugin.git.Sync`
- `io.kestra.plugin.git.SyncDashboards`
- `io.kestra.plugin.git.SyncFlow`
- `io.kestra.plugin.git.SyncFlows`
- `io.kestra.plugin.git.SyncNamespaceFiles`
- `io.kestra.plugin.git.TenantSync`

### Project Structure

```
plugin-git/
├── src/main/java/io/kestra/plugin/git/services/
├── src/test/java/io/kestra/plugin/git/services/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
