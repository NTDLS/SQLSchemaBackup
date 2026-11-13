WITH cteTables([Schema], [Table], [ObjectId], Ordinal) AS
(
	SELECT
		s.name AS [Schema],
		OBJECT_NAME(so.object_id) AS [Table],
		so.object_id AS [ObjectId],
		0 AS Ordinal
	FROM
		sys.objects AS so
	INNER JOIN sys.schemas as s
		ON s.schema_id = so.schema_id
	WHERE
		so.type = 'U'
		AND so.is_ms_Shipped = 0
	UNION ALL
	SELECT
		s.name AS [Schema],
		OBJECT_NAME(so.object_id) AS [Table],
		so.object_id AS [ObjectId],
		tt.Ordinal + 1 AS Ordinal
	FROM
		sys.objects AS so
	INNER JOIN sys.schemas as s
		ON s.schema_id = so.schema_id
	INNER JOIN sys.foreign_keys AS f
		ON f.parent_object_id = so.object_id
		AND f.parent_object_id != f.referenced_object_id
	INNER JOIN cteTables AS tt
		ON f.referenced_object_id = tt.[ObjectId]
	WHERE
		so.type = 'U'
		AND so.is_ms_Shipped = 0
)

SELECT DISTINCT
	t.Ordinal,
	t.[Schema],
	t.[Table]
FROM
	cteTables AS t
INNER JOIN
	(
		SELECT
			itt.[Schema],
			itt.[Table],
			itt.[ObjectId],
			Max(itt.Ordinal) as Ordinal
		FROM
			cteTables AS itt
		GROUP BY
			itt.[Schema],
			itt.[Table],
			itt.[ObjectId]
	) AS tt
	ON t.[ObjectId] = tt.[ObjectId]
	AND t.Ordinal = tt.Ordinal
WHERE
	t.[Table] NOT IN ('sysdiagrams')
ORDER BY
	t.Ordinal,
	t.[Schema],
	t.[Table]
