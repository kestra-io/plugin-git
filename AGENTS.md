# Kestra Git Plugin

## What

Integrate Git for efficient data workflows in Kestra. Exposes 13 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Git, allowing orchestration of Git-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
