---
description: Drive the SqlClone live demo (demo/ folder). Usage: /demo [setup|clone|verify|drift|snapshot|reset|full]
---

Drive the self-contained SqlClone demo described in `demo/README.md`: a local `demo-source`
container plays the shared dev environment, and the tool clones it into `sqlclone-demo`.
Use the committed scripts and the SqlClone CLI — do not hand-roll docker/SQL steps that a
script already covers.

The user's request: $ARGUMENTS

## Invocation rules (both matter — failures are confusing without them)

- Every `dotnet run` gets `--no-launch-profile` (launchSettings.json otherwise injects the Development clone args).
- Subcommands without an `--environment` option (`teardown`, `generate-seed-config`, `inspect-source`, `validate`) read `DOTNET_ENVIRONMENT`. Set `$env:DOTNET_ENVIRONMENT='Demo'` in the same shell invocation — shell state does not persist between separate tool calls.
- Run everything from the repo root.

## Connection reference

| Container | Port | Auth |
|---|---|---|
| demo-source | `localhost,14999` | sa / `ScoutHub!Demo1` |
| sqlclone-demo | `localhost,14433` | sa / `SqlClone!Demo1` |

Query via `docker exec <container> /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P <password> -d ScoutHub -Q "..."`.

## Modes (pick from $ARGUMENTS; `full` = setup → clone → verify → drift)

- **setup** — `powershell -NoProfile -File demo/setup-demo-source.ps1` (add `-Recreate` if the user wants a clean slate). Confirms: 32 teams / 500 players / 50,000 reports loaded, drift objects planted.
- **clone** — first check `Clone:Seed:Tables` in `src/SqlClone.Console/appsettings.Demo.json`. **If it is empty**, run `generate-seed-config --source-database ScoutHub` (with `DOTNET_ENVIRONMENT=Demo`), then merge the emitted `Tables` into the config with the two demo tunings: drop the `__EFMigrationsHistory` entry, and set `"LatestRows": 10000, "LatestOrderBy": "[CreatedUtc] DESC, [Id] DESC"` on `ScoutingReports`. Then `dotnet run --no-launch-profile --project src/SqlClone.Console -- clone --environment Demo`. Expect ~15 s.
- **verify** — query sqlclone-demo: ScoutingReports count (expect exactly 10000), orphan checks on ScoutingReports→Players, Players→Teams, WorkoutResults→Workouts (expect 0 each), and `SELECT TOP 3 ... FROM vw_TopProspects` (proves the drift view exists and runs).
- **drift** — show the drift story: list the three planted objects on demo-source (`vw_TopProspects`, `usp_GetRecentReports`, `fn_GetLegacyFallGrade`), confirm none appear in `demo/ScoutHub/Migrations/`, and point at the clone log line `Programmable-object sync: creating N missing object(s)`.
- **snapshot** — stop sqlclone-demo, `docker commit --change 'ENV ACCEPT_EULA=Y' --change 'ENV MSSQL_SA_PASSWORD=' sqlclone-demo scouthub-localdb:demo`, restart sqlclone-demo, then prove the image: run a throwaway container on port 14533, wait for readiness, query the report count, and remove it.
- **reset** — teardown the clone (`DOTNET_ENVIRONMENT=Demo` + `teardown`), then re-run setup with `-Recreate`.

## Reporting

Report what a presenter needs: per-step success, the seeded row counts, the
programmable-object sync line verbatim, validation status, and wall-clock time for the
clone. A healthy clone ends with `Clone completed with status Success`.
