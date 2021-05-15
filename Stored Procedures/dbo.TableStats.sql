CREATE OR ALTER   procedure [dbo].[TableStats]
	@tablemast varchar(max),@where varchar(max)=''
as
begin

	--declare @tablemast varchar(max)=''
	--declare @where varchar(max)=''
	--exec dbo.TableStats 'dbo.Customers'
	--exec dbo.TableStats '#temppai'
	set @tablemast=replace(replace(@tablemast,'[',''),']','')
	declare @table varchar(255)= iif(charindex('.',@tablemast)>0,substring(@tablemast,charindex('.',@tablemast)+1,255),@tablemast)
	declare @schema varchar(255)=iif(charindex('.',@tablemast)>0,substring(@tablemast,1,charindex('.',@tablemast)-1),null)
	print(@table)
	print(@schema)

	if @schema is null
	begin
		set @schema=isnull(@schema,(
		SELECT top 1 s.name
		from sys.tables AS t
		left join sys.all_columns c ON T.object_id = c.object_id
		left join sys.types TY ON c.system_type_id = TY.system_type_id AND c.user_type_id = TY.user_type_id
		left join sys.schemas as s on s.schema_id=t.schema_id
		WHERE T.is_ms_shipped = 0
		and t.name  like @table ))
	end
	print(@schema)

	declare @columnid varchar(3)=''
	declare @columnName varchar(255)=''
	declare @sqlcommand nvarchar(max)=''


	drop table if exists #Distribution
	Create table #Distribution
	(
		Ranking int,
		colName nvarchar(255),
		oldcolName nvarchar(255),
		colVal varchar(max),
	)

	drop table if exists #tablemap
	SELECT
		 c.column_id
		,c.name AS 'ColumnName'
		,s.name as 'schemaName'
	into #tablemap
	from sys.tables AS t
	left join sys.all_columns c ON T.object_id = c.object_id
	left join sys.types TY ON c.system_type_id = TY.system_type_id AND c.user_type_id = TY.user_type_id
	left join sys.schemas as s on s.schema_id=t.schema_id
	WHERE T.is_ms_shipped = 0
	and t.name  like @table and s.name like @schema
	union
	SELECT
		 c.column_id
		,c.name
		,s.name as 'schemaName'
	from sys.all_views T
	left join sys.all_columns c ON T.object_id = c.object_id
	left join sys.types TY ON c.system_type_id = TY.system_type_id AND c.user_type_id = TY.user_type_id
	left join sys.schemas as s on s.schema_id=t.schema_id
	WHERE T.is_ms_shipped = 0
	and t.name  like @table and s.name like @schema
	union
	SELECT
		 c.column_id
		,c.name
		,s.name as 'schemaName'
	FROM tempdb.sys.tables AS t
	     JOIN tempdb.sys.all_columns c ON T.object_id = c.object_id
	     JOIN tempdb.sys.types TY ON c.system_type_id = ty.system_type_id AND c.user_type_id = ty.user_type_id
	     join tempdb.sys.schemas as s on s.schema_id=t.schema_id
	WHERE T.is_ms_shipped = 0
	and t.name like @table+'%' and CHARINDEX('#',@table)>=1


	DECLARE DBcursor CURSOR FOR
	select column_id
		  ,ColumnName
	from #tableMap


	OPEN DBcursor; FETCH DBcursor INTO @columnid,@columnName; WHILE (@@FETCH_STATUS = 0)
	BEGIN
		set @sqlcommand='
			;with t1 as (
				select
					row_number() over(order by case when [@columnName] is null then -1 when cast([@columnName] as nvarchar(max)) ='''' then 0 else 1 end ,count(*) desc) as Ranking
					,replace(replace(''@Columnname'',''['',''''),'']'','''')+'' (''+(select cast(max(isnull(len(convert(nvarchar(max),[@Columnname],25)),0))as varchar) from @schema.@table)+'')'' as colname
					,replace(replace(''@Columnname '',''['',''''),'']'','''') as oldcolname
					,left(''[''+cast(count(*) as varchar)+'']           '',11)+'' ''+isnull(convert(nvarchar(max),[@Columnname],25),''NULL'') as colval
				from @schema.@table
				@where
				group by [@columnName]
			)
			insert into #Distribution(Ranking,colName,oldcolname,colVal)
			select
				 Ranking
				,colName
				,oldcolname
				,colVal
			from t1
			where Ranking<=20
		'
		set @sqlcommand = iif(CHARINDEX('#',@table)>=1,replace(@sqlcommand, '@schema.@table', @table),@sqlcommand)
		set @sqlcommand = replace(@sqlcommand, '@table', @table)
		set @sqlcommand = replace(@sqlcommand, '@where', @where)
		set @sqlcommand = replace(@sqlcommand, '@schema', isnull(@schema,''))
		set @sqlcommand = replace(@sqlcommand, '@columnName', @Columnname)
		exec(@sqlcommand)
		FETCH DBcursor INTO @columnid,@columnName
	END
	CLOSE DBCURSOR; DEALLOCATE DBCURSOR



	declare @cols nvarchar(max);

	set @sqlCommand=(select '
	select distinct
		@cols = string_agg(''[''+colName+'']'','','') within group (order by column_id asc)
	from (
	select distinct
		replace(replace(colname,''[]'',''''),'']'',''''),column_id
	from #Distribution as a
	left join #tablemap as b on b.columnname=a.oldcolname)a(colname,column_id)')
	exec sp_executeSQl @sqlCommand,N'@cols nvarchar(max) output',@cols output
	alter table #distribution
	drop column oldcolname
	set @sqlCommand='
	select Ranking,@cols
	from #Distribution
	pivot
	(
		max(colval)
		for colname in(@cols)
	)as pivottable
	'
	set @sqlcommand=replace(@sqlcommand,'@cols',@cols)
	exec(@sqlcommand)


end

GO
