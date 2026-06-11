-- "Drift" objects for the demo: programmable objects that exist on the source
-- database but in NO ScoutHub migration. The clone's programmable-object sync
-- should detect these as missing from the migrated schema and recreate them.
-- (Don't show this file to the audience until the drift-reveal beat.)
USE ScoutHub;
GO

CREATE OR ALTER VIEW dbo.vw_TopProspects
AS
SELECT TOP (50)
    p.Id AS PlayerId,
    p.FirstName,
    p.LastName,
    p.Position,
    t.City,
    t.Name AS TeamName,
    AVG(sr.Grade) AS AverageGrade,
    COUNT(*) AS ReportCount
FROM dbo.Players p
JOIN dbo.Teams t ON t.Id = p.TeamId
JOIN dbo.ScoutingReports sr ON sr.PlayerId = p.Id
GROUP BY p.Id, p.FirstName, p.LastName, p.Position, t.City, t.Name
ORDER BY AVG(sr.Grade) DESC;
GO

CREATE OR ALTER PROCEDURE dbo.usp_GetRecentReports
    @PlayerId int,
    @Count int = 10
AS
BEGIN
    SET NOCOUNT ON;
    SELECT TOP (@Count)
        sr.Id,
        sr.ScoutName,
        sr.Grade,
        sr.Notes,
        sr.CreatedUtc
    FROM dbo.ScoutingReports sr
    WHERE sr.PlayerId = @PlayerId
    ORDER BY sr.CreatedUtc DESC;
END;
GO

CREATE OR ALTER FUNCTION dbo.fn_GetLegacyFallGrade (@PlayerId int)
RETURNS decimal(4, 2)
AS
BEGIN
    DECLARE @grade decimal(4, 2);
    SELECT @grade = AVG(sr.Grade)
    FROM dbo.ScoutingReports sr
    WHERE sr.PlayerId = @PlayerId
      AND MONTH(sr.CreatedUtc) IN (9, 10, 11);
    RETURN COALESCE(@grade, 0);
END;
GO
