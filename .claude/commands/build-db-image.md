---
description: Regenerate the local SQL clone and optionally publish it as an image to ACR. Usage: /build-db-image [push]
---

Rebuild the local SQL Server clone with the SqlClone tool, and — when asked — commit and
push it to an Azure Container Registry. Drive everything through the committed PowerShell
commands in `scripts/`; do not hand-roll the docker/dotnet steps.

The user's request: $ARGUMENTS

## Commands

- **Regenerate only** (teardown → clone → migrate → seed → sync programmable objects → validate):

  ```
  powershell -NoProfile -File scripts/regenerate-local-db.ps1
  ```

- **Regenerate, then commit + push to ACR** (delegates to build-and-push-image.ps1):

  ```
  powershell -NoProfile -File scripts/regenerate-local-db.ps1 -AcrName <registry-short-name>
  ```

  Add `-Tag <tag>` / `-NoLatest` to control the published tag. Ask the user for the
  registry name if they said "push" without naming one.

## How to run it

1. **Prerequisites** — confirm: Docker Desktop running; `dotnet` (and, for a push, `az`)
   on PATH; a configured `src/SqlClone.Console/appsettings.{Environment}.json` (plus
   gitignored `appsettings.Local.json` for any secrets — see `appsettings.Local.example.json`).
2. **Choose the mode** from `$ARGUMENTS`: if the user wants to publish/push, run with
   `-AcrName`; otherwise regenerate only.
3. **Run it in the background** if the configured source is large; monitor the log.
4. **Report results**: seed completed/failed counts, the programmable-object sync summary,
   validation status, and — if pushed — the tags that landed.

A clone can be healthy with a handful of known per-table failures if validation succeeds;
surface them rather than hiding them, but don't treat them as a failed run.
