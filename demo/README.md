# SqlClone Live Demo — Presenter Runbook

A self-contained, fully local demo of the SqlClone pipeline. Two SQL Server containers
and a stub app:

```
┌──────────────────────────┐                   ┌──────────────────────────┐
│  demo-source  (:14999)   │                   │  sqlclone-demo  (:14433) │
│  plays the shared dev    │ ──── SqlClone ──▶ │  the disposable local    │
│  environment (e.g. an    │  migrate + seed   │  copy every dev gets     │
│  Azure SQL MI)           │  + drift-sync     │                          │
└──────────────────────────┘                   └──────────────────────────┘
            ▲
   demo/ScoutHub — stub app whose EF Core migrations
   are the schema's source of truth (3 migrations)
```

Everything runs on localhost — no VPN, no cloud, no network needed once images are pulled.
The fact that the "shared dev environment" is just a connection string in
`appsettings.Demo.json` is itself part of the demo: swap it for a real Azure SQL MI and
nothing else changes.

**Fictional data:** 32 invented teams, 500 generated players, 50,000 scouting reports.
Nothing real appears on screen.

## One-time setup (before the talk)

```powershell
# from the repo root
powershell -NoProfile -File demo/setup-demo-source.ps1
```

Stands up `demo-source` (SQL Server 2022 on port 14999), builds the ScoutHub schema by
running its three EF migrations, loads the fictional data, and **plants the drift
objects** — `vw_TopProspects`, `usp_GetRecentReports`, and `fn_GetLegacyFallGrade`, which
exist on the source but in **no migration**. Re-run any time to reset (add `-Recreate`
for a clean container). Takes ~1–2 minutes.

Pre-pull `mcr.microsoft.com/mssql/server:2022-latest` so nothing downloads during the talk.

## The demo beats

> **Gotcha #1:** subcommands without an `--environment` option (`generate-seed-config`,
> `teardown`, `validate`, `inspect-source`) resolve the environment from
> `DOTNET_ENVIRONMENT`. Set it once per terminal session:
>
> ```powershell
> $env:DOTNET_ENVIRONMENT = 'Demo'
> ```
>
> **Gotcha #2:** add `--no-launch-profile` to every `dotnet run` so
> `Properties/launchSettings.json` (which defaults to the Development clone) doesn't
> interfere.

All commands run from the repo root.

### Beat 1 — the cast
```powershell
docker ps                                    # demo-source = "the shared dev MI"
```
Show `demo/ScoutHub/Migrations/` — three migrations, schema as source code.
Browse demo-source in SSMS/ADS (`localhost,14999`, sa / `ScoutHub!Demo1`): 50,000 reports.
Don't dwell on the Views/Programmability folders.

### Beat 2 — the tool reads the source
```powershell
dotnet run --no-launch-profile --project src/SqlClone.Console -- generate-seed-config --source-database ScoutHub
```
It infers the FK tree without being told: Teams (Order 10) → Players (20) →
ScoutingReports + Workouts (30) → WorkoutResults (40), nested as `Children`.
Then show the tuned result in `appsettings.Demo.json`: `__EFMigrationsHistory` removed,
`LatestRows: 10000` on ScoutingReports. *"Generated, then tuned — nobody hand-writes this."*

### Beat 3 — the clone, live (~15 seconds)
```powershell
dotnet run --no-launch-profile --project src/SqlClone.Console -- clone --environment Demo
```
Narrate the log as it streams: container start → materialize empty DB (*"schema comes
from source code, not from a copy"*) → migrations → seed lanes with row counts → **the
programmable-object sync line goes by — say nothing** → post-clone → validation.

### Beat 4 — inspect the clone
Connect to `localhost,14433` (sa / `SqlClone!Demo1`), or:
```powershell
docker exec sqlclone-demo /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'SqlClone!Demo1' -d ScoutHub -Q "
SELECT 'ScoutingReports' AS T, COUNT(*) AS Rows FROM ScoutingReports;
SELECT COUNT(*) AS OrphanReports FROM ScoutingReports sr LEFT JOIN Players p ON p.Id = sr.PlayerId WHERE p.Id IS NULL;"
```
10,000 rows, not 50,000 — and zero orphans. *"Top-N on the parent, recursive FK filtering
down the children. Small database, still referentially coherent."*

### Beat 5 — the drift reveal
Show `vw_TopProspects` on **demo-source**, then show ScoutHub's migrations: it's nowhere.
Scroll back to the clone log:
```
Programmable-object sync: creating 3 missing object(s) in ScoutHub (1 view(s), 1 procedure(s), 1 function(s))
```
Prove they work in the clone:
```powershell
docker exec sqlclone-demo /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'SqlClone!Demo1' -d ScoutHub -Q "SELECT TOP 3 LastName, Position, AverageGrade FROM vw_TopProspects ORDER BY AverageGrade DESC;"
```
*"Every clone run is a drift report."* Optional encore: `CREATE VIEW` something new on
demo-source live, re-run clone, watch it get picked up.

### Beat 6 — disposability + distribution
```powershell
$env:DOTNET_ENVIRONMENT = 'Demo'
dotnet run --no-launch-profile --project src/SqlClone.Console -- teardown   # cattle, not pets
dotnet run --no-launch-profile --project src/SqlClone.Console -- clone --environment Demo
```
Then the snapshot (what `scripts/build-and-push-image.ps1` automates):
```powershell
docker stop sqlclone-demo
docker commit --change 'ENV ACCEPT_EULA=Y' --change 'ENV MSSQL_SA_PASSWORD=' sqlclone-demo scouthub-localdb:demo
docker start sqlclone-demo
docker run -d --name snapshot-test -p 14533:1433 scouthub-localdb:demo
# ~20s later: a complete, seeded database — that's new-dev onboarding
docker exec snapshot-test /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'SqlClone!Demo1' -d ScoutHub -Q "SELECT COUNT(*) FROM ScoutingReports"
docker rm -f snapshot-test
```

## Reset / fallback

- Reset data + drift on the source: re-run `demo/setup-demo-source.ps1`
- Clean slate everywhere: `teardown` (with `DOTNET_ENVIRONMENT=Demo`) + `setup-demo-source.ps1 -Recreate`
- Keep a pre-built `scouthub-localdb:demo` image and a stopped, pre-cloned `sqlclone-demo`
  container as the fallback if a live step misbehaves

## Ports & credentials (demo only — invented, safe to show)

| What | Where | Auth |
|---|---|---|
| demo-source (the "MI") | `localhost,14999` | sa / `ScoutHub!Demo1` |
| sqlclone-demo (the clone) | `localhost,14433` | sa / `SqlClone!Demo1` |
| snapshot-test (Beat 6) | `localhost,14533` | sa / `SqlClone!Demo1` |

Verified timings on a typical dev laptop: full from-scratch clone **~15 s**; source setup
**~1–2 min**; snapshot container ready **~20 s**.
