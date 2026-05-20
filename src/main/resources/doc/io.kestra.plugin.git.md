# How to use the Git plugin

Most Git tasks run inside a `WorkingDirectory` task so that cloned files are available to downstream tasks in the same execution.

For HTTPS authentication, set `username` and `password` — use a personal access token as the `password` value for GitHub, GitLab, and Bitbucket rather than your account password. For SSH, set `privateKey` to the PEM-encoded key content (not a file path) and `passphrase` if the key is protected. Store both in [secrets](https://kestra.io/docs/concepts/secret).

### How-to guides

#### Clone a repo and run a script

Wrap `Clone` and subsequent tasks inside `WorkingDirectory` so they share the same filesystem. After cloning, all repository files are available to downstream tasks in the same working directory.

```yaml
id: run_from_repo
namespace: company.team

tasks:
  - id: wdir
    type: io.kestra.plugin.core.flow.WorkingDirectory
    tasks:
      - id: clone
        type: io.kestra.plugin.git.Clone
        url: https://github.com/your-org/your-repo
        branch: main
        username: git_username
        password: "{{ secret('GITHUB_TOKEN') }}"

      - id: run_script
        type: io.kestra.plugin.scripts.python.Commands
        commands:
          - python scripts/process.py
```

`depth: 1` (the default) performs a shallow clone. Set `commit` to check out a specific SHA, or `tag` to check out a tag — both disable shallow cloning.

#### Push namespace files and flows to Git

`Push` commits files from the working directory, [namespace files](https://kestra.io/docs/concepts/namespace-files), or flow definitions to a remote branch. Use it to keep Kestra configuration in version control.

```yaml
id: backup_to_git
namespace: company.team

tasks:
  - id: push
    type: io.kestra.plugin.git.Push
    url: https://github.com/your-org/kestra-config
    branch: main
    username: git_username
    password: "{{ secret('GITHUB_TOKEN') }}"
    commitMessage: "chore: sync namespace files {{ now() }}"
    namespaceFiles:
      enabled: true
    flows:
      enabled: true
      childNamespaces: true
```

`flows.gitDirectory` (default: `_flows`) controls where flow YAML files are written in the repository. Use `namespaceFiles.include` and `namespaceFiles.exclude` to filter which files are pushed.

#### Sync namespace files from Git

`SyncNamespaceFiles` is the pull-direction counterpart to `Push` — it fetches files from a Git branch and applies them to a Kestra namespace. Use it for GitOps workflows where Git is the source of truth for your namespace files.

```yaml
id: sync_namespace_files
namespace: company.team

tasks:
  - id: sync
    type: io.kestra.plugin.git.SyncNamespaceFiles
    url: https://github.com/your-org/kestra-config
    branch: main
    username: git_username
    password: "{{ secret('GITHUB_TOKEN') }}"
    gitDirectory: _files
    namespace: company.team
    delete: true
```

Set `delete: true` to remove namespace files that no longer exist in the Git branch. The equivalent task for flows is `SyncFlows`.
