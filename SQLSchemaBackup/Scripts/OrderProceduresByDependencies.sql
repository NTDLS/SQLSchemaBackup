WITH cteProcedures([Schema], [Procedure], [ObjectId], Ordinal) AS
(
	SELECT
		S.name AS [Schema],
		OBJECT_NAME(SO.object_id) AS [Procedure],
		SO.object_id AS [ObjectId],
		0 AS Ordinal
	FROM
		sys.objects AS SO
	INNER JOIN sys.schemas as S
		ON S.schema_id = SO.schema_id
	WHERE
		SO.type = 'P'
		AND SO.is_ms_Shipped = 0
	UNION ALL
	SELECT
		S.name AS [Schema],
		OBJECT_NAME(SO.object_id) AS [Procedure],
		SO.object_id AS [ObjectId],
		TT.Ordinal + 1 AS Ordinal
	FROM
		sys.objects AS SO
	INNER JOIN sys.schemas as S
		ON S.schema_id = SO.schema_id
	INNER JOIN sys.sql_dependencies AS F
		ON F.referenced_major_id = SO.object_id
		AND F.referenced_major_id != F.object_id
	INNER JOIN cteProcedures AS TT
		ON F.object_id = TT.[ObjectId]
	WHERE
		SO.type = 'P'
		AND SO.is_ms_Shipped = 0
)

SELECT DISTINCT
	T.Ordinal,
	T.[Schema],
	T.[Procedure]
FROM
	cteProcedures AS T
INNER JOIN
	(
		SELECT
			ITT.[Schema],
			ITT.[Procedure],
			ITT.[ObjectId],
			MAX(ITT.Ordinal) as Ordinal
		FROM
			cteProcedures AS ITT
		GROUP BY
			ITT.[Schema],
			ITT.[Procedure],
			ITT.[ObjectId]
	) AS tt
	ON T.[ObjectId] = tt.[ObjectId]
	AND T.Ordinal = tt.Ordinal
WHERE
	T.[Procedure] NOT IN ('sp_alterdiagram','sp_creatediagram','sp_dropdiagram','sp_helpdiagramdefinition','sp_helpdiagrams','sp_renamediagram','sp_upgraddiagrams')
ORDER BY
	T.Ordinal DESC,
	T.[Schema],
	T.[Procedure]
