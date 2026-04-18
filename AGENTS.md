# Kestra Git Plugin

## What

- Provides plugin components under `io.kestra.plugin.git`.
- Includes classes such as `SyncFlow`, `Sync`, `SyncNamespaceFiles`, `PushNamespaceFiles`.

## Why

- This plugin integrates Kestra with Git.
- It provides tasks that clone, fetch, and interact with Git repositories.

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
