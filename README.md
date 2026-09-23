<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png"  alt="Kestra workflow orchestrator" />
  </a>
</p>

<h1 align="center" style="border-bottom: none">
    Event-Driven Declarative Orchestrator
</h1>

<div align="center">
 <a href="https://github.com/kestra-io/kestra/releases"><img src="https://img.shields.io/github/tag-pre/kestra-io/kestra.svg?color=blueviolet" alt="Last Version" /></a>
  <a href="https://github.com/kestra-io/kestra/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/kestra-io/kestra?color=blueviolet" alt="License" /></a>
  <a href="https://github.com/kestra-io/kestra/stargazers"><img src="https://img.shields.io/github/stars/kestra-io/kestra?color=blueviolet&logo=github" alt="Github star" /></a> <br>
<a href="https://kestra.io"><img src="https://img.shields.io/badge/Website-kestra.io-192A4E?color=blueviolet" alt="Kestra infinitely scalable orchestration and scheduling platform"></a>
<a href="https://kestra.io/slack"><img src="https://img.shields.io/badge/Slack-Join%20Community-blueviolet?logo=slack" alt="Slack"></a>
</div>

<br />

<p align="center">
  <a href="https://twitter.com/kestra_io" style="margin: 0 10px;">
        <img src="https://kestra.io/twitter.svg" alt="twitter" width="35" height="25" /></a>
  <a href="https://www.linkedin.com/company/kestra/" style="margin: 0 10px;">
        <img src="https://kestra.io/linkedin.svg" alt="linkedin" width="35" height="25" /></a>
  <a href="https://www.youtube.com/@kestra-io" style="margin: 0 10px;">
        <img src="https://kestra.io/youtube.svg" alt="youtube" width="35" height="25" /></a>
</p>

<br />
<p align="center">
    <a href="https://go.kestra.io/video/product-overview" target="_blank">
        <img src="https://kestra.io/startvideo.png" alt="Get started in 3 minutes with Kestra" width="640px" />
    </a>
</p>
<p align="center" style="color:grey;"><i>Get started with Kestra in 3 minutes.</i></p>

# Kestra Git plugin

## Why

- What user problem does this solve? Teams need to clone, fetch, and interact with Git repositories from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Git steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Git.

## What

- Provides plugin components under `io.kestra.plugin.git`.
- Includes classes such as `SyncFlow`, `Sync`, `SyncNamespaceFiles`, `PushNamespaceFiles`.

## Dashboards

Dashboard tasks (`SyncDashboards`, `PushDashboards`) moved to the Git EE plugin in Kestra 2.0.0, because dashboards are an Enterprise Edition feature. `TenantSync` no longer syncs the `_global/dashboards` directory on OSS.

## Source of truth overrides

`TenantSync` and `NamespaceSync` set their sync direction with `sourceOfTruth` (`KESTRA` pushes Kestra state to Git, `GIT` applies Git state into Kestra). `sourceOfTruthOverrides` lets you set the direction **per resource kind**, so one run can push one kind to Git while pulling the other kind from Git in the same execution. It currently exposes `flows` and `namespaceFiles`; any field left unset falls back to `sourceOfTruth`.

```yaml
- id: sync
  type: io.kestra.plugin.git.TenantSync
  sourceOfTruth: KESTRA          # flows: Kestra -> Git
  sourceOfTruthOverrides:
    namespaceFiles: GIT          # namespace files: Git -> Kestra
  whenMissingInSource: KEEP
  branch: main
  url: https://github.com/my-org/my-repo
```

`whenMissingInSource` stays a single global setting, but its effect flips per kind with the resolved source. With `sourceOfTruth: KESTRA`, `sourceOfTruthOverrides.namespaceFiles: GIT`, and `whenMissingInSource: DELETE`, a Namespace File present in Kestra but absent from Git is deleted **from Kestra** (Git is the source for files), while a flow present in Git but absent from Kestra is deleted **from Git** (Kestra is the source for flows). `protectedNamespaces` still guards every deletion regardless of direction.

## Documentation
* Full documentation can be found under [kestra.io/docs](https://kestra.io/docs)
* Documentation for developing a plugin is included in the [Plugin Developer Guide](https://kestra.io/docs/plugin-developer-guide/).


## License
Apache 2.0 © [Kestra Technologies](https://kestra.io)


## Stay up to date

We release new versions every month. Give the [main repository](https://github.com/kestra-io/kestra) a star to stay up to date with the latest releases and get notified about future updates.

![Star the repo](https://kestra.io/star.gif)
