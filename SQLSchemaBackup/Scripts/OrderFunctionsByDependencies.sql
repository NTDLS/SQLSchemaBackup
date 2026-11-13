WITH cteFunctions([Schema], [Function], [ObjectId], Ordinal) AS
(
	SELECT
		S.name AS [Schema],
		OBJECT_NAME(SO.object_id) AS [Function],
		SO.object_id AS [ObjectId],
		0 AS Ordinal
	FROM
		sys.objects AS SO
	INNER JOIN sys.schemas as S
		ON S.schema_id = SO.schema_id
	WHERE
		SO.type IN ('FN', 'FT', 'IF', 'TF')
		AND SO.is_ms_Shipped = 0
	UNION ALL
	SELECT
		S.name AS [Schema],
		OBJECT_NAME(SO.object_id) AS [Function],
		SO.object_id AS [ObjectId],
		tt.Ordinal + 1 AS Ordinal
	FROM
		sys.objects AS SO
	INNER JOIN sys.schemas as S
		ON S.schema_id = SO.schema_id
	INNER JOIN sys.sql_dependencies AS F
		ON F.referenced_major_id = SO.object_id
		AND F.referenced_major_id != F.object_id
	INNER JOIN cteFunctions AS tt
		ON F.object_id = tt.[ObjectId]
	WHERE
		SO.type IN ('FN', 'FT', 'IF', 'TF')
		AND SO.is_ms_Shipped = 0
)

SELECT DISTINCT
	T.Ordinal,
	T.[Schema],
	T.[Function]
FROM
	cteFunctions AS T
INNER JOIN
	(
		SELECT
			ITT.[Schema],
			ITT.[Function],
			ITT.[ObjectId],
			MAX(ITT.Ordinal) as Ordinal
		FROM
			cteFunctions AS ITT
		GROUP BY
			ITT.[Schema],
			ITT.[Function],
			ITT.[ObjectId]
	) AS TT
	ON T.[ObjectId] = TT.[ObjectId]
	AND T.Ordinal = TT.Ordinal
WHERE
	T.[Function] NOT IN ('fn_diagramobjects')
ORDER BY
	T.Ordinal DESC,
	T.[Schema],
	T.[Function]
