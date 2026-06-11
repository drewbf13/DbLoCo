-- Loads fictional scouting data into the ScoutHub database on the demo-source container.
-- All names are invented; volumes are sized so the clone's Top-N subsetting is visible
-- (50,000 scouting reports, of which the demo clone seeds only the latest 10,000).
USE ScoutHub;
GO

SET NOCOUNT ON;

DELETE FROM WorkoutResults;
DELETE FROM Workouts;
DELETE FROM ScoutingReports;
DELETE FROM Players;
DELETE FROM Teams;

-- 32 fictional teams: 2 conferences x 4 divisions x 4 teams
WITH cities AS (
    SELECT v.n, v.city, v.mascot
    FROM (VALUES
        (1,'Ashford','Stallions'),(2,'Brookmont','Talons'),(3,'Cedar Falls','Mariners'),(4,'Dunmore','Sentinels'),
        (5,'Eastvale','Thunder'),(6,'Fairhaven','Wolfpack'),(7,'Glenrock','Comets'),(8,'Harborview','Knights'),
        (9,'Ironwood','Bisons'),(10,'Junction City','Rattlers'),(11,'Kingsbridge','Falcons'),(12,'Lakemont','Storm'),
        (13,'Midvale','Outlaws'),(14,'Northgate','Huskies'),(15,'Oakcrest','Raptors'),(16,'Pinehurst','Generals'),
        (17,'Quarry Bay','Sharks'),(18,'Riverton','Blizzard'),(19,'Stonebrook','Cougars'),(20,'Telford','Aviators'),
        (21,'Umberland','Grizzlies'),(22,'Vandermeer','Phantoms'),(23,'Westcliff','Chargers'),(24,'Yorkfield','Drifters'),
        (25,'Zephyr Hills','Stampede'),(26,'Alderton','Vipers'),(27,'Bellgrave','Monarchs'),(28,'Crestwood','Pioneers'),
        (29,'Dorchester','Wolverines'),(30,'Elmsworth','Cyclones'),(31,'Foxborough Pines','Rangers'),(32,'Graymoor','Titans of Industry')
    ) AS v(n, city, mascot)
)
INSERT INTO Teams (City, Name, Conference, Division)
SELECT city,
       mascot,
       CASE WHEN n <= 16 THEN 'American' ELSE 'National' END,
       CASE (n - 1) / 4 % 4 WHEN 0 THEN 'East' WHEN 1 THEN 'North' WHEN 2 THEN 'South' ELSE 'West' END
FROM cities;

-- 500 players spread across the 32 teams
WITH firsts AS (
    SELECT v.i, v.name FROM (VALUES
        (0,'Marcus'),(1,'Devon'),(2,'Tyler'),(3,'Jalen'),(4,'Cody'),(5,'Andre'),(6,'Brock'),(7,'Xavier'),
        (8,'Trent'),(9,'Malik'),(10,'Hunter'),(11,'Darius'),(12,'Cole'),(13,'Isaiah'),(14,'Gavin'),(15,'Trevon'),
        (16,'Logan'),(17,'Quentin'),(18,'Reese'),(19,'Solomon')
    ) AS v(i, name)
),
lasts AS (
    SELECT v.i, v.name FROM (VALUES
        (0,'Whitfield'),(1,'Crandall'),(2,'Osei'),(3,'Bartowski'),(4,'Lindqvist'),(5,'Marsh'),(6,'Okafor'),(7,'Delacroix'),
        (8,'Hargrove'),(9,'Stenson'),(10,'Vance'),(11,'Kowalski'),(12,'Brightwater'),(13,'Aldana'),(14,'Pemberton'),(15,'Ruiz'),
        (16,'Thackeray'),(17,'Mbeki'),(18,'Sorenson'),(19,'Gallagher'),(20,'Redmond'),(21,'Castellanos'),(22,'Finch'),(23,'Drummond'),(24,'Ellsworth')
    ) AS v(i, name)
),
positions AS (
    SELECT v.i, v.pos FROM (VALUES
        (0,'QB'),(1,'RB'),(2,'WR'),(3,'WR'),(4,'TE'),(5,'OT'),(6,'OG'),(7,'C'),
        (8,'EDGE'),(9,'DT'),(10,'LB'),(11,'CB'),(12,'CB'),(13,'S'),(14,'K'),(15,'P')
    ) AS v(i, pos)
),
colleges AS (
    SELECT v.i, v.college FROM (VALUES
        (0,'Harmon State'),(1,'Calloway Tech'),(2,'University of Brandt'),(3,'Ridgeline A&M'),(4,'Mercer Valley'),
        (5,'Holloway College'),(6,'Pacific Crest'),(7,'Fort Sumner'),(8,'Easton University'),(9,'Greenbriar State')
    ) AS v(i, college)
)
INSERT INTO Players (TeamId, FirstName, LastName, Position, College, HeightInches, WeightPounds)
SELECT tm.Id,
       f.name,
       l.name,
       p.pos,
       c.college,
       68 + s.value % 12,
       180 + (s.value * 7) % 140
FROM GENERATE_SERIES(1, 500) AS s
JOIN (SELECT Id, ROW_NUMBER() OVER (ORDER BY Id) AS rn FROM Teams) tm ON tm.rn = 1 + (s.value - 1) % 32
JOIN firsts f ON f.i = s.value % 20
JOIN lasts l ON l.i = s.value % 25
JOIN positions p ON p.i = s.value % 16
JOIN colleges c ON c.i = s.value % 10;

-- 50,000 scouting reports over the last ~3 seasons
WITH scouts AS (
    SELECT v.i, v.scout FROM (VALUES
        (0,'D. Hatcher'),(1,'R. Yamada'),(2,'P. Okonkwo'),(3,'L. Severin'),(4,'M. Castile'),
        (5,'J. Brandt'),(6,'A. Whitlock'),(7,'S. Moreau'),(8,'K. Tanaka'),(9,'C. Abernathy')
    ) AS v(i, scout)
)
INSERT INTO ScoutingReports (PlayerId, ScoutName, Grade, Notes, CreatedUtc)
SELECT pl.Id,
       sc.scout,
       CAST(4.00 + (s.value * 13 % 600) / 100.0 AS decimal(4,2)),
       CONCAT('Session ', s.value, ': film review and live evaluation notes.'),
       DATEADD(MINUTE, -(s.value * 31), SYSUTCDATETIME())
FROM GENERATE_SERIES(1, 50000) AS s
JOIN (SELECT Id, ROW_NUMBER() OVER (ORDER BY Id) AS rn FROM Players) pl ON pl.rn = 1 + s.value % 500
JOIN scouts sc ON sc.i = s.value % 10;

-- ~1,000 workouts (2 per player), 4 drill results each
INSERT INTO Workouts (PlayerId, WorkoutDate, Location)
SELECT pl.Id,
       DATEADD(DAY, -(s.value % 365), CAST(SYSUTCDATETIME() AS date)),
       CASE s.value % 4 WHEN 0 THEN 'Indoor Facility' WHEN 1 THEN 'Pro Day' WHEN 2 THEN 'Combine' ELSE 'Campus Visit' END
FROM GENERATE_SERIES(1, 1000) AS s
JOIN (SELECT Id, ROW_NUMBER() OVER (ORDER BY Id) AS rn FROM Players) pl ON pl.rn = 1 + s.value % 500;

WITH drills AS (
    SELECT v.i, v.drill, v.base FROM (VALUES
        (0,'40-Yard Dash',4.3),(1,'Bench Press',15.0),(2,'Vertical Jump',28.0),(3,'3-Cone Drill',6.7)
    ) AS v(i, drill, base)
)
INSERT INTO WorkoutResults (WorkoutId, Drill, Result)
SELECT wo.Id,
       d.drill,
       CAST(d.base + (wo.rn * 17 % 100) / 50.0 AS decimal(18,2))
FROM (SELECT Id, ROW_NUMBER() OVER (ORDER BY Id) AS rn FROM Workouts) wo
CROSS JOIN drills d;

SELECT 'Teams' AS TableName, COUNT(*) AS Rows FROM Teams
UNION ALL SELECT 'Players', COUNT(*) FROM Players
UNION ALL SELECT 'ScoutingReports', COUNT(*) FROM ScoutingReports
UNION ALL SELECT 'Workouts', COUNT(*) FROM Workouts
UNION ALL SELECT 'WorkoutResults', COUNT(*) FROM WorkoutResults;
GO
