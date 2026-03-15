# SqlClone v1 (DbLoCo)

SqlClone is a .NET 10 console tool that provisions a local SQL Server Docker container representing a cloned Azure SQL/MI development environment.

## Prerequisites

- Windows development machine (first-class target for v1)
- .NET 10 SDK
- Docker Desktop with Linux containers enabled
- Access to a source SQL endpoint for `inspect-source`

## Solution Layout

- `SqlClone.sln`
- `src/SqlClone.Console` - entry point and command wiring (System.CommandLine)
- `src/SqlClone.Application` - orchestration and plan creation
- `src/SqlClone.Domain` - options/models/interfaces
- `src/SqlClone.Infrastructure` - dependency injection composition root
- `src/SqlClone.Infrastructure.Docker` - docker CLI lifecycle management
- `src/SqlClone.Infrastructure.SqlServer` - SQL helpers/materialization/validation
- `tests/SqlClone.Tests` - baseline unit tests

## Configuration

Configuration sources (in order):

1. `appsettings.json`
2. `appsettings.{Environment}.json`
3. `appsettings.Local.json` (optional override)
4. Environment variables

Use `appsettings.example.json` as a template.

Important settings:

- `Clone:Docker:ContainerName`
- `Clone:Docker:Image`
- `Clone:Docker:HostPort`
- `Clone:Docker:SaPassword`
- `Clone:Source:ConnectionString`
- `Clone:Restore:Materializer` (`CreateEmpty` or `NoOp`)
- `Clone:Restore:Databases`
- `Clone:LinkedServers:Definitions`
- `Clone:PostClone:ScriptFolders`

## Docker + SQL Server notes

v1 shells out to `docker` CLI commands (`inspect`, `run`, `start`, `stop`, `rm`).

When cloning:

- the tool ensures the configured container exists/runs
- waits for SQL readiness by opening SQL connections repeatedly
- materializes configured databases
- applies linked servers
- executes post-clone scripts (`*.sql`) in lexical order
- validates SQL reachability, database existence, and linked server existence

## Commands

```bash
dotnet run --project src/SqlClone.Console -- init
dotnet run --project src/SqlClone.Console -- inspect-source
dotnet run --project src/SqlClone.Console -- clone --environment Development
dotnet run --project src/SqlClone.Console -- validate
dotnet run --project src/SqlClone.Console -- teardown
```

## Known v1 limitations

- No real Azure backup export/restore implementation yet
- No bacpac automation
- No login/user remapping workflow
- No secret vault integration
- No parallel restore pipeline
- No linked server login mappings

## Next steps

- Add lane/environment profile layering and immutable clone plans
- Implement real materializers (backup/restore or bacpac workflows)
- Add security-focused secret providers
- Add richer validation/report persistence
- Add integration tests that run against ephemeral containers
