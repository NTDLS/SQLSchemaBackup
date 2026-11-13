WITH cteViews([Schema], [View], TableID, Ordinal) AS
(
	SELECT
		s.name AS [Schema],
		OBJECT_NAME(so.object_id) AS [View],
		so.object_id AS TableID,
		0 AS Ordinal
	FROM
		sys.objects AS so
	inner join sys.schemas as s
		on s.schema_id = so.schema_id
	WHERE
		so.type = 'V'
		AND so.is_ms_Shipped = 0
	UNION ALL
	SELECT
		s.name AS [Schema],
		OBJECT_NAME(so.object_id) AS [View],
		so.object_id AS TableID,
		tt.Ordinal + 1 AS Ordinal
	FROM
		sys.objects AS so
	inner join sys.schemas as s
		on s.schema_id = so.schema_id
	INNER JOIN sys.sql_dependencies AS f
		ON f.referenced_major_id = so.object_id
		AND f.referenced_major_id != f.object_id
	INNER JOIN cteViews AS tt
		ON f.object_id = tt.TableID
	WHERE
		so.type = 'V'
		AND so.is_ms_Shipped = 0
)

SELECT DISTINCT
	t.Ordinal,
	t.[Schema],
	t.[View]
FROM
	cteViews AS t
INNER JOIN
	(
		SELECT
			itt.[Schema],
			itt.[View],
			itt.TableID,
			MAX(itt.Ordinal) as Ordinal
		FROM
			cteViews AS itt
		GROUP BY
			itt.[Schema],
			itt.[View],
			itt.TableID
	) AS tt
	ON t.TableID = tt.TableID
	AND t.Ordinal = tt.Ordinal
ORDER BY
	t.Ordinal DESC,
	t.[Schema],
	t.[View]


