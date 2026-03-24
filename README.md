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
- `Clone:Source:EnableAlwaysEncrypted` (set `true` when source tables use Always Encrypted columns)
- `Clone:Source:Encrypt` / `Clone:Source:TrustServerCertificate` (optional overrides applied on top of the source connection string)
- `Clone:Source:DisableConnectionPooling` (set `true` to test/mitigate TLS session reuse issues)
- `Clone:Restore:Materializer` (`CreateEmpty`, `AzureBackup`, or `NoOp`)
- `Clone:Restore:AzureBackup:BackupUrlTemplate`
- `Clone:Restore:AzureBackup:SharedAccessSignature` (optional, auto-generated with current user identity when omitted)
- `Clone:Restore:AzureBackup:SqlCredentialName`
- `Clone:Restore:Databases`
- `Clone:Migration:Enabled` / (`GitRepository` or `LocalRepositoryPath`) / `Branch` / `BuildCommand`
- `Clone:Seed:Enabled` / `SourceDatabase` / `Tables`
  - Optional seeding strategy switch: `Clone:Seed:Strategy` (`BulkCopy` or `LinkedServer`)
  - Linked-server strategy setting: `Clone:Seed:LinkedServerName`
- `Clone:LinkedServers:Definitions`
  - Optional SQL auth mapping per linked server: `UserId` + `Password`
- `Clone:PostClone:ScriptFolders`

## Docker + SQL Server notes

v1 shells out to `docker` CLI commands (`inspect`, `run`, `start`, `stop`, `rm`).

When cloning:

- the tool ensures the configured container exists/runs
- waits for SQL readiness by opening SQL connections repeatedly
- materializes configured databases
- applies linked servers
- runs configured migrations/seeding steps
- executes post-clone scripts (`*.sql`) in lexical order
- validates SQL reachability, database existence, and linked server existence

## Quick answer: copy an Azure SQL Managed Instance DB into local Docker

If your goal is "take database X from SQL MI and run it locally in Docker", the v1 path is:

1. **Take/locate a `.bak` backup for each database** you want from Managed Instance in Azure Blob Storage.
2. **Set materializer to `AzureBackup`** in `appsettings.Local.json`.
3. **Configure `BackupUrlTemplate` + SAS** so SQL Server in Docker can read each `.bak` over HTTPS.
4. **Run `clone`** to create/start the container and run `RESTORE ... FROM URL` for each DB.

Example `Clone:Restore` snippet:

```json
"Restore": {
  "Materializer": "AzureBackup",
  "Databases": [ "AppDb" ],
  "AzureBackup": {
    "BackupUrlTemplate": "https://<storage>.blob.core.windows.net/sql-backups/{sourceServer}/{database}.bak",
    "SharedAccessSignature": "?sv=...",
    "SqlCredentialName": "SqlCloneAzureBackupSas"
  }
}
```

Then run:

```bash
dotnet run --project src/SqlClone.Console -- clone --environment Development
```

Notes:

- `CreateEmpty` does **not** copy MI data; it creates blank DBs only.
- `NoOp` leaves DB materialization to you.
- The local container must be able to reach Blob Storage, and the SQL credential/SAS must permit blob read access.

### How to create the `.bak` in Azure Blob from SQL Managed Instance

From SQL Managed Instance, you can write backups directly to Blob Storage with `BACKUP DATABASE ... TO URL`.

1. Create a Blob container (for example: `sql-backups`).
2. Generate a SAS token for that container with **Write** + **List** (for backup) and later **Read** (for restore).
3. In the MI `master` database, create a SQL credential that uses the SAS token.
4. Run `BACKUP DATABASE ... TO URL` for each database you want to clone.

Example in MI:

```sql
USE [master];
GO

-- Name can be any SQL credential name; keep it simple and reusable.
CREATE CREDENTIAL [https://<storage-account>.blob.core.windows.net/sql-backups]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<sas-token-without-leading-question-mark>';
GO

BACKUP DATABASE [AppDb]
TO URL = 'https://<storage-account>.blob.core.windows.net/sql-backups/<mi-server-name>/AppDb.bak'
WITH COPY_ONLY, COMPRESSION, CHECKSUM, STATS = 10;
GO
```

After the backup completes, point `Clone:Restore:AzureBackup:BackupUrlTemplate` to the same Blob path pattern, and provide a SAS that local SQL Server can read.

### Alternative: export/import (BACPAC or data movement)

If your goal is schema + data portability (instead of native SQL backup semantics), export/import can be simpler:

- **BACPAC**: export from Azure SQL Managed Instance and import into your local SQL Server instance/container.
- **Data movement**: use ETL/copy tooling to move schema + data directly.

When deciding between approaches:

- Use **native backup/restore** when you want SQL Server backup fidelity (`.bak` + restore behavior).
- Use **BACPAC** when you want a portable package and can accept that it is a logical export/import workflow.
- Microsoft notes BACPAC export of a **TDE-protected** database is supported, and the exported BACPAC content is not protected by TDE encryption.

For this project today, BACPAC/data-movement execution is not automated in `SqlClone` v1 (see Known limitations), so run those steps outside the tool and use `NoOp`/`CreateEmpty` as needed.


### Migration + seed workflow (source repo + Azure SQL seed tables)

If you can build schema migrations from source code, configure SqlClone like this:

1. Set `Clone:Restore:Materializer` to `CreateEmpty` (or `NoOp`) so the target DB exists without depending on native backup restore.
2. Enable `Clone:Migration` with either a git repo + branch **or** a local repository path + migration command.
3. Enable `Clone:Seed` and list tables to copy from Azure SQL source into your local DB after migration runs.

Example:

```json
"Migration": {
  "Enabled": true,
  "GitRepository": "https://github.com/your-org/your-db-migrations.git",
  "LocalRepositoryPath": "",
  "Branch": "main",
  "BuildCommand": "dotnet run --project tools/DbMigrate -- --connection \"Server=localhost,14333;Database=AppDb;User Id=sa;Password=YourStrong!Passw0rd;Encrypt=False;TrustServerCertificate=True\"",
  "WorkingDirectory": ""
},
"Seed": {
  "Enabled": true,
  "SourceDatabase": "AppDb",
  "ExcludeSchemas": [],
  "Tables": [
    {
      "SourceDatabase": "AppDb",
      "TargetDatabase": "AppDb",
      "Schema": "dbo",
      "Table": "ReferenceData",
      "TruncateTarget": true,
      "LatestRows": 10000,
      "LatestOrderBy": "[CreatedUtc] DESC, [Id] DESC",
      "Order": 10,
      "GroupKey": 1,
      "Children": [
        {
          "Table": "ReferenceDataTranslations"
        }
      ]
    }
  ]
}
```

The clone sequence becomes: start container -> materialize DB -> apply linked servers -> run the configured migration command from the configured repository/branch -> seed configured tables from Azure SQL source -> post-clone scripts/validation.

If your migrations project is already on your machine, set `Clone:Migration:LocalRepositoryPath` and leave `GitRepository` blank. SqlClone will run `BuildCommand` directly from that local path (plus `WorkingDirectory`, if set) and skip git clone.

`BuildCommand` is intentionally generic: SqlClone executes it once and treats your migration repo/tooling as the source of truth. Common patterns:

- **Apply directly via tool code** (for example a custom migrator app):
  - `dotnet run --project tools/DbMigrate -- --connection "..."`
- **Generate a SQL script, then execute it** (works well when devs already use EF tools):
  - `dotnet ef migrations script --project src/App.Data --startup-project src/App.Api --output artifacts/migrate.sql --no-transactions && sqlcmd -S localhost,14333 -d AppDb -U sa -P "YourStrong!Passw0rd" -I -i artifacts/migrate.sql -b`

If SQL Server returns `CREATE INDEX failed ... SET options have incorrect settings: 'QUOTED_IDENTIFIER'`, make sure your `sqlcmd` invocation includes `-I` so quoted identifiers are enabled for the session that runs the script.

Prefer the **non-idempotent** script shape above for local clone provisioning. This avoids SQL Server batch/parser edge cases that can occur with idempotent wrappers around module DDL (for example `CREATE VIEW`).

### Troubleshooting: `SSL Provider ... The specified data could not be decrypted`

This error is usually a **TLS transport/session** problem, not SQL table encryption metadata. Practical steps:

1. Keep `Encrypt=True` for cloud SQL sources and validate endpoint/certificate chain.
2. Upgrade client/driver/runtime so `Microsoft.Data.SqlClient` and TLS stack are current.
3. If your source uses **Always Encrypted** columns, enable client-side decryption with:

```json
"Source": {
  "ConnectionString": "...",
  "EnableAlwaysEncrypted": true
}
```

`EnableAlwaysEncrypted` sets SqlClient `Column Encryption Setting=Enabled` for source connections so supported encrypted column values can be read during seed operations.

If you already have `Column Encryption Setting=Enabled` in the connection string and still see this SSL decrypt transport error, verify the machine can access the CMK provider (for example cert store/Key Vault) and that SQL auth/TLS transport settings are valid independently of Always Encrypted.

4. If the same source endpoint intermittently fails only during repeated clone/seed reads, disable source connection pooling for diagnosis:

```json
"Source": {
  "DisableConnectionPooling": true
}
```

5. If you need to force TLS flags regardless of the base connection string, set:

```json
"Source": {
  "Encrypt": true,
  "TrustServerCertificate": false
}
```

This script-first pattern is usually simpler than having SqlClone dynamically load a referenced `DbContext` and call `Migrate()` itself, because it keeps SqlClone decoupled from application assemblies, runtime versions, and design-time `DbContext` wiring.

Ordering notes:

- **Migration ordering** is controlled by your migration tool/repo (via `BuildCommand`). SqlClone invokes that command once.
- **Seed ordering** is controlled by `Clone:Seed:Tables[*]:Order` (ascending). Tables in the same order value are seeded in parallel.
- **Seed grouping metadata** is provided by `Clone:Seed:Tables[*]:GroupKey` (for example domain/schema lanes in generated config) and is used as a deterministic tie-breaker.
- **Seed row limiting** can be configured with `Clone:Seed:Tables[*]:LatestRows` plus optional `LatestOrderBy` (defaults to primary key descending). Child tables that reference limited parent tables are automatically filtered to rows whose foreign keys exist in the parent's selected set, and this is applied recursively through deeper parent/child chains.
- **Nested seed config** is supported via `Clone:Seed:Tables[*]:Children`. Child entries inherit source/target/schema/order/group defaults from their parent unless overridden.
- **Schema exclusion** can be configured with `Clone:Seed:ExcludeSchemas` to skip seeding all tables from listed schemas.
- **Index handling during seed import**: SqlClone temporarily disables nonclustered indexes on seeded target tables before importing rows, then rebuilds them after seeding completes. Primary-key/unique-constraint-backed indexes and foreign key constraints remain enabled.
- **Post-clone scripts** still run in lexical file name order.

Parallelism notes:

- Seeding runs in **dependency levels** keyed by `Order` (or `GroupKey` when `Order` is not set). Only tables in the same level run concurrently.
- If you observe only one running seed task, it usually means each table resolved to a different dependency level (or unresolved cyclic tables were intentionally split into single-table levels).
- A failed table does not permanently reduce concurrency by itself; retries stay within that table's slot and other tables in the same level continue running up to the configured max.

### Selective Top N seeding per table

You can now selectively copy only the latest **Top N** rows for specific tables while leaving other tables fully copied.

- Set `LatestRows` on a table to enable Top N filtering for that table.
- Set `LatestOrderBy` when you want explicit recency ordering (for example by `CreatedUtc` then `Id`).
- Leave `LatestRows` unset (or `0`) to copy the full table.

Example:

```json
"Seed": {
  "Enabled": true,
  "SourceDatabase": "AppDb",
  "ExcludeSchemas": [ "audit" ],
  "Tables": [
    {
      "SourceDatabase": "AppDb",
      "TargetDatabase": "AppDb",
      "Schema": "dbo",
      "Table": "AuditEvents",
      "TruncateTarget": true,
      "LatestRows": 5000,
      "LatestOrderBy": "[CreatedUtc] DESC, [EventId] DESC",
      "Order": 30,
      "GroupKey": 2
    },
    {
      "SourceDatabase": "AppDb",
      "TargetDatabase": "AppDb",
      "Schema": "dbo",
      "Table": "ReferenceData",
      "TruncateTarget": true,
      "Order": 10,
      "GroupKey": 1
    }
  ]
}
```

In this example, `AuditEvents` is limited to the latest 5,000 rows, while `ReferenceData` is copied in full.

### Alternative seeding strategy: linked-server pushdown (with normal SqlClone logging)

If you want seeding to execute through linked-server SQL on the target instance **and still use the normal SqlClone seeding pipeline/logging**, set:

```json
"Seed": {
  "Enabled": true,
  "Strategy": "LinkedServer",
  "LinkedServerName": "REMOTEDEV",
  "SourceDatabase": "AppDb",
  "Tables": [
    {
      "TargetDatabase": "AppDb",
      "Schema": "dbo",
      "Table": "ReferenceData",
      "TruncateTarget": true
    }
  ]
}
```

With `Strategy: LinkedServer`, SqlClone still orchestrates seeding as usual (ordering/retries/logging), but each table seed operation stages rows with `SELECT ... INTO #temp` from the linked server and inserts into the target table using only shared writable columns.

At a high level, linked-server pushdown uses patterns like:

```sql
-- source metadata
SELECT COLUMN_NAME
FROM [SOURCE_LINKED].[AppDb].INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'ReferenceData';

-- target metadata
SELECT c.name, c.is_identity, c.is_computed
FROM [AppDb].sys.columns c
JOIN [AppDb].sys.tables t ON c.object_id = t.object_id
JOIN [AppDb].sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dbo' AND t.name = 'ReferenceData';

-- stage source rows server-side
SELECT shared_columns
INTO #Seed_ReferenceData
FROM [SOURCE_LINKED].[AppDb].[dbo].[ReferenceData];

-- then insert into target with shared columns only
INSERT INTO [AppDb].[dbo].[ReferenceData](shared_columns)
SELECT shared_columns
FROM #Seed_ReferenceData;
```

This keeps the linked-server path inside the same `clone`/seed execution path so you still get structured table-level progress logs.

## Commands

```bash
dotnet run --project src/SqlClone.Console -- init
dotnet run --project src/SqlClone.Console -- inspect-source
dotnet run --project src/SqlClone.Console -- generate-seed-config --source-database AppDb
dotnet run --project src/SqlClone.Console -- clone --environment Development
dotnet run --project src/SqlClone.Console -- validate
dotnet run --project src/SqlClone.Console -- teardown
```

`generate-seed-config` emits a JSON `Seed` section with `Order` values computed from foreign-key dependency levels (parents before children), plus `GroupKey` values grouped by schema/domain to keep related tables together while still maximizing per-level parallelism. Root entries default `LatestRows` to `10000`, child entries default to full-copy (`LatestRows` unset), and `LatestOrderBy` is emitted from each table's primary key columns in descending order when a primary key exists. Generated output is nested using `Children` so parent/child intent is explicit in the config. When a table references multiple possible parents, nesting now prefers domain-ownership signals (table-name affinity like `workout_*` and less lookup-like parents referenced by many tables) before falling back to FK order. Use `--target-database` to override target DB name and `--truncate-target false` if you want generated entries to keep existing rows.

## Known v1 limitations

- Azure backup restore requires accessible backup blobs and SQL Server support for `RESTORE ... FROM URL`
- No bacpac automation
- No login/user remapping workflow
- No secret vault integration
- No parallel restore pipeline

## Next steps

- Add lane/environment profile layering and immutable clone plans
- Implement real materializers (backup/restore or bacpac workflows)
- Add security-focused secret providers
- Add richer validation/report persistence
- Add integration tests that run against ephemeral containers

## First-launch checklist

1. Run `dotnet run --project src/SqlClone.Console -- init` to verify Docker CLI and create local config stubs when missing.
2. Copy/update `src/SqlClone.Console/appsettings.Local.json` with a real source connection string and secure SA password override.
3. Run `dotnet run --project src/SqlClone.Console -- clone --environment Development`.

Environment-specific config files are selected from the `--environment` command argument at startup, then overlaid with `appsettings.Local.json` and environment variables.
