/*
  Linked-server seed strategy: copy data from a source SQL Server into a local target DB.

  Workflow:
  1) Ensure the linked server exists (sp_addlinkedserver + login mapping).
  2) For each table, compute column intersection (source vs target).
  3) If target table exists, INSERT matching columns.
  4) If target table does not exist, SELECT ... INTO to create it from source.

  Notes:
  - Four-part names are used: [LinkedServer].[SourceDatabase].[Schema].[Table]
  - Computed columns in target are excluded automatically.
  - Identity insert can be toggled.
*/

USE [master];
GO

IF OBJECT_ID('dbo.usp_SeedTableFromLinkedServer', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_SeedTableFromLinkedServer;
GO

CREATE PROCEDURE dbo.usp_SeedTableFromLinkedServer
      @LinkedServer sysname
    , @SourceDatabase sysname
    , @TargetDatabase sysname
    , @SchemaName sysname
    , @TableName sysname
    , @TruncateTarget bit = 0
    , @IncludeIdentity bit = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @targetObjectId int;
    DECLARE @targetThreePart nvarchar(600) = QUOTENAME(@TargetDatabase) + N'.' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName);
    DECLARE @sourceFourPart nvarchar(800) = QUOTENAME(@LinkedServer) + N'.' + QUOTENAME(@SourceDatabase) + N'.' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName);

    DROP TABLE IF EXISTS #TargetColumns;
    DROP TABLE IF EXISTS #SourceColumns;
    DROP TABLE IF EXISTS #SharedColumns;

    CREATE TABLE #TargetColumns
    (
        ColumnName sysname NOT NULL PRIMARY KEY,
        IsIdentity bit NOT NULL,
        IsComputed bit NOT NULL
    );

    CREATE TABLE #SourceColumns
    (
        ColumnName sysname NOT NULL PRIMARY KEY
    );

    CREATE TABLE #SharedColumns
    (
        ColumnName sysname NOT NULL PRIMARY KEY,
        IsIdentity bit NOT NULL
    );

    DECLARE @targetMetadataSql nvarchar(max) = N'
SELECT c.name AS ColumnName,
       CONVERT(bit, c.is_identity) AS IsIdentity,
       CONVERT(bit, c.is_computed) AS IsComputed
FROM ' + QUOTENAME(@TargetDatabase) + N'.sys.columns c
JOIN ' + QUOTENAME(@TargetDatabase) + N'.sys.tables t ON c.object_id = t.object_id
JOIN ' + QUOTENAME(@TargetDatabase) + N'.sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName
  AND t.name = @TableName;';

    INSERT INTO #TargetColumns (ColumnName, IsIdentity, IsComputed)
    EXEC sp_executesql
          @targetMetadataSql
        , N'@SchemaName sysname, @TableName sysname'
        , @SchemaName = @SchemaName
        , @TableName = @TableName;

    SET @targetObjectId = OBJECT_ID(@targetThreePart);

    DECLARE @sourceMetadataSql nvarchar(max) = N'
SELECT c.COLUMN_NAME
FROM ' + QUOTENAME(@LinkedServer) + N'.' + QUOTENAME(@SourceDatabase) + N'.INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = @SchemaName
  AND c.TABLE_NAME = @TableName;';

    INSERT INTO #SourceColumns (ColumnName)
    EXEC sp_executesql
          @sourceMetadataSql
        , N'@SchemaName sysname, @TableName sysname'
        , @SchemaName = @SchemaName
        , @TableName = @TableName;

    INSERT INTO #SharedColumns (ColumnName, IsIdentity)
    SELECT t.ColumnName,
           t.IsIdentity
    FROM #TargetColumns t
    INNER JOIN #SourceColumns s
        ON s.ColumnName = t.ColumnName
    WHERE t.IsComputed = 0
      AND (@IncludeIdentity = 1 OR t.IsIdentity = 0);

    IF NOT EXISTS (SELECT 1 FROM #SharedColumns)
    BEGIN
        THROW 50001, 'No compatible writable columns between source and target table.', 1;
    END

    DECLARE @columnList nvarchar(max) =
    (
        SELECT STRING_AGG(QUOTENAME(ColumnName), N', ') WITHIN GROUP (ORDER BY ColumnName)
        FROM #SharedColumns
    );

    DECLARE @insertSql nvarchar(max) = N'';
    DECLARE @toggleIdentity bit = CASE WHEN @IncludeIdentity = 1 AND EXISTS (SELECT 1 FROM #SharedColumns WHERE IsIdentity = 1) THEN 1 ELSE 0 END;

    IF @targetObjectId IS NULL
    BEGIN
        SET @insertSql = N'
SELECT ' + @columnList + N'
INTO ' + @targetThreePart + N'
FROM ' + @sourceFourPart + N';';
    END
    ELSE
    BEGIN
        IF @TruncateTarget = 1
        BEGIN
            SET @insertSql += N'TRUNCATE TABLE ' + @targetThreePart + N';' + CHAR(10);
        END

        IF @toggleIdentity = 1
            SET @insertSql += N'SET IDENTITY_INSERT ' + @targetThreePart + N' ON;' + CHAR(10);

        SET @insertSql += N'
INSERT INTO ' + @targetThreePart + N' (' + @columnList + N')
SELECT ' + @columnList + N'
FROM ' + @sourceFourPart + N';' + CHAR(10);

        IF @toggleIdentity = 1
            SET @insertSql += N'SET IDENTITY_INSERT ' + @targetThreePart + N' OFF;' + CHAR(10);
    END

    PRINT N'Seeding [' + @SchemaName + N'].[' + @TableName + N'] using columns: ' + @columnList;
    EXEC (@insertSql);
END;
GO

/*
Example setup + usage

-- 1) Add linked server (run once)
EXEC master.dbo.sp_addlinkedserver
      @server = N'SOURCE_SQL'
    , @srvproduct = N''
    , @provider = N'MSOLEDBSQL'
    , @datasrc = N'source-sql-host,1433';

EXEC master.dbo.sp_addlinkedsrvlogin
      @rmtsrvname = N'SOURCE_SQL'
    , @useself = N'False'
    , @locallogin = NULL
    , @rmtuser = N'seed_reader'
    , @rmtpassword = N'<password>';

-- 2) Seed table while respecting column differences
EXEC master.dbo.usp_SeedTableFromLinkedServer
      @LinkedServer = N'SOURCE_SQL'
    , @SourceDatabase = N'AppDb'
    , @TargetDatabase = N'AppDb'
    , @SchemaName = N'dbo'
    , @TableName = N'ReferenceData'
    , @TruncateTarget = 1
    , @IncludeIdentity = 1;
*/
