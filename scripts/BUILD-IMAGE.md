# Building a seeded SQL Server image and pushing it to ACR

This produces a Docker image that already contains your **fully provisioned + seeded
database(s)**, so teammates can `docker pull` a ready-to-use local DB instead of each
running the multi-minute clone pipeline themselves.

## Why a plain `docker commit` works here

The clone tool starts the container with **no volume mount** (see
`DockerSqlContainerManager.EnsureStartedAsync`), and the official
`mcr.microsoft.com/mssql/server` image declares **no `VOLUME`** for `/var/opt/mssql`.
So all seeded data sits in the container's writable layer and is captured by
`docker commit`. (If a future change adds a `-v` mount or the base image starts
declaring a volume, this approach breaks and you'd need a backup/restore-based image
instead.)

## Prerequisites

- Docker Desktop running (Linux containers)
- .NET 10 SDK (only if you let the script run the clone step)
- Azure CLI (`az`) with an `az login` session that can **push** to the target ACR
  (`AcrPush` role or equivalent)
- A working `appsettings.Local.json` / `appsettings.{Environment}.json` (valid source
  connection string + SA password), i.e. the same config you use to run `clone`

## One command

From `scripts/`:

```powershell
./build-and-push-image.ps1 -AcrName <acr-short-name>
```

That runs the full flow: **clone → stop container → commit → tag → `az acr login` → push**,
tagging both `:<yyyyMMdd>` and `:latest`.

Common variations:

```powershell
# Custom tag, and snapshot the container that's already provisioned (skip the clone step)
./build-and-push-image.ps1 -AcrName myregistry -Tag release-candidate -SkipClone

# Build + tag locally but don't log in / push (dry run of everything but the upload)
./build-and-push-image.ps1 -AcrName myregistry -SkipPush

# Only push the dated tag, not :latest, and leave the local container running afterwards
./build-and-push-image.ps1 -AcrName myregistry -NoLatest -KeepRunning
```

| Parameter        | Default            | Purpose                                                        |
| ---------------- | ------------------ | -------------------------------------------------------------- |
| `-AcrName`       | *(required)*       | Registry short name; login server = `<AcrName>.azurecr.io`     |
| `-DbImage`       | `sqlclone-localdb` | Repository name                                                |
| `-Tag`           | today's UTC date   | Image tag                                                      |
| `-ContainerName` | `sqlclone-local`   | Must match `Clone:Docker:ContainerName`                        |
| `-Environment`   | `Development`      | Selects `appsettings.<env>.json` for the clone step            |
| `-SkipClone`     | off                | Snapshot the existing container instead of re-running clone    |
| `-SkipPush`      | off                | Build + tag only; no `az acr login` / `docker push`            |
| `-NoLatest`      | off                | Don't also tag/push `:latest`                                  |
| `-KeepRunning`   | off                | Restart the local container after the snapshot                 |

## Manual equivalent

If you'd rather run it by hand (or in CI), this is what the script does:

```powershell
# 1. Provision + seed the local container
dotnet run --project src/SqlClone.Console -- clone --environment Development

# 2. Stop it so SQL Server flushes (consistent snapshot)
docker stop sqlclone-local

# 3. Commit the seeded container to an image
docker commit `
  --change "ENV ACCEPT_EULA=Y" `
  --change "ENV MSSQL_SA_PASSWORD=" `
  sqlclone-local sqlclone-localdb:20260608

# 4. Tag for ACR
docker tag sqlclone-localdb:20260608 <acr>.azurecr.io/sqlclone-localdb:20260608

# 5. Log in and push
az acr login --name <acr>
docker push <acr>.azurecr.io/sqlclone-localdb:20260608
```

## Consuming the image

```powershell
docker pull <acr>.azurecr.io/sqlclone-localdb:20260608
docker run -d --name sqlclone-local -p 14333:1433 <acr>.azurecr.io/sqlclone-localdb:20260608
```

Authenticate with the SA password that was active during `clone`
(`Clone:Docker:SaPassword`) — that value is baked into the seeded `master` database,
so it's what consumers authenticate with regardless of any `MSSQL_SA_PASSWORD` env
passed at run time.

## Notes & cautions

- **Image size**: a seeded image is large (multiple GB). The first push and pulls take
  a while; ACR storage is consumed per tag — prune old tags periodically.
- **Push only to a private registry.** The image contains real data and the
  baked-in SA password; it must not go to a public registry.
- **Consistency**: the script stops the container before committing so SQL Server shuts
  down cleanly. Don't commit a container mid-seed.
- **Secrets**: `MSSQL_SA_PASSWORD` is scrubbed from the image's env metadata, but the
  password still lives inside the seeded data by design. Treat the image as sensitive.
