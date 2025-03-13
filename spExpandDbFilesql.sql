-- testing blah blah blah
select getdate()
go

use DBA

if object_id('spExpandDbFile', N'P') is not null
  drop procedure spExpandDbFile;
go

-----------------------------------------------------------------------------------------------------------
-- exec DBA.dbo.spExpandDbFile @debug = 1
-- exec DBA.dbo.spExpandDbFile @db = 'Support', @debug = 1
-- 
-- File size allocation
--  XXsmall [ < 50 mb ]			--> add   2 Mb	( 70% full)
--  Xsmall  [50 Mb - 1 Gb]		--> add  20 Mb	( 75% full)
--   small  [1 Gb - 50 Gb]		--> add 200 Mb	( 80% full)
--  medium  [50 Gb - 200 Gb]	--> add   1 Gb	(85% full & < 5 Gb free)
--  large	[200 Gb - 1 Tb]     --> add   2 Gb	(90% full & < 10 Gb free)
--  Xlarge  [ > 1 Tb]			--> add   5 Gb	(95% full & < 15 Gb free)
--
-----------------------------------------------------------------------------------------------------------
create procedure spExpandDbFile
  @db sysname = null,
  @debug bit = 0
as

set nocount on

if @db is not null and not exists (select * from sys.databases where name = @db) begin
  raiserror ('Database [%s] does not exist!', 16, 1, @db)
  return
end

declare 
  @xxsmall int = 50,
  @xsmall int = 1 * 1024,
  @small int = 50 * 1024,
  @medium int = 300 * 1024,
  @large int = 1024 * 1024,
  @cmd nvarchar(2000) = ''

if OBJECT_ID('tempdb..#dbfiles', N'U') is not null
  drop table #dbfiles

create table #dbfiles (
  database_name sysname,
  type_desc varchar(15),
  logical_file_name varchar(125),
  physical_name varchar(250),
  size_mb decimal(10,2),
  used_mb decimal(10,2),
  free_mb decimal(10,2),
  used_percent decimal(10,2),
  modify_file varchar(500)
)

if OBJECT_ID('DBA.dbo.DbFileGrowth', N'U') is null begin
  select * into DBA.dbo.DbFileGrowth from #dbfiles where 1 = 0
  alter table DBA.dbo.DbFileGrowth add rdate datetime default getdate()
end

-- get db file's stats
if @db is not null begin
  select @cmd =
  'USE [' + @db + '];
  select 
    DB_NAME()	as database_name,
    type_desc,
    name,
    physical_name,
    convert(decimal(10,2), size/128.0) as size_mb,
    convert(decimal(10,2), FILEPROPERTY(name, ''SpaceUsed'')/128.0) as used_mb
  from sys.database_files with(nolock)'  
  insert into #dbfiles (database_name, type_desc, logical_file_name, physical_name, size_mb, used_mb)
  exec sp_executesql @cmd
end
else begin
  insert into #dbfiles (database_name, type_desc, logical_file_name, physical_name, size_mb, used_mb)
  exec sp_MSforeachdb
  'USE [?];
  select 
    DB_NAME()	as database_name,
    type_desc,
    name,
    physical_name,
    convert(decimal(10,2), size/128.0) as size_mb,
    convert(decimal(10,2), FILEPROPERTY(name, ''SpaceUsed'')/128.0) as used_mb
  from sys.database_files with(nolock)'
end

update #dbfiles set free_mb = size_mb - used_mb
update #dbfiles set used_percent = convert(decimal(10,2), ((used_mb*1.0 / size_mb) * 100)) where size_mb > 0
delete #dbfiles where database_name = 'tempdb'  -- exclude tempdb

if @db is not null and exists (select name from sys.databases where name = @db)
  delete #dbfiles where database_name <> @db

-- xxsmall files --> add 2 Mb
update #dbfiles
set modify_file = 'ALTER DATABASE [' + database_name + '] MODIFY FILE (NAME = ' + logical_file_name + ', SIZE = ' + CAST((convert(int,size_mb) + 2) AS varchar(25)) + 'MB);'
where size_mb < @xxsmall
  and used_percent > 70

-- xsmall files --> add 20 Mb
update #dbfiles
set modify_file = 'ALTER DATABASE [' + database_name + '] MODIFY FILE (NAME = ' + logical_file_name + ', SIZE = ' + CAST((convert(int,size_mb) + 20) AS varchar(25)) + 'MB);'
where size_mb between @xxsmall and @xsmall
  and used_percent > 75

-- small files  --> add 200 Mb
update #dbfiles
set modify_file =  'ALTER DATABASE [' + database_name + '] MODIFY FILE (NAME = ' + logical_file_name + ', SIZE = ' + CAST((convert(int,size_mb) + 200) AS varchar(25)) + 'MB);'
where size_mb between @xsmall and @small
  and used_percent > 80
  and free_mb < 2*1024

-- medium files  --> add 1Gb
update #dbfiles
set modify_file = 'ALTER DATABASE [' + database_name + '] MODIFY FILE (NAME = ' + logical_file_name + ', SIZE = ' + CAST((convert(int,size_mb) + 1024) AS varchar(25)) + 'MB);'
where size_mb between @small and @medium
  and used_percent > 85
  and free_mb < 5*1024

-- large files --> add 2Gb
update #dbfiles
set modify_file = 'ALTER DATABASE [' + database_name + '] MODIFY FILE (NAME = ' + logical_file_name + ', SIZE = ' + CAST((convert(int,size_mb) + 2*1024) AS varchar(25)) + 'MB);'
where size_mb between @medium and @large
  and used_percent > 90
  and free_mb < 10*1024

-- xlarge files > 1 Tb -- add 5Gb
update #dbfiles
set modify_file = 'ALTER DATABASE [' + database_name + '] MODIFY FILE (NAME = ' + logical_file_name + ', SIZE = ' + CAST((convert(int,size_mb) + 5*1024) AS varchar(25)) + 'MB);'
where size_mb > @large
  and used_percent > 95
  and free_mb < 15*1024

if OBJECT_ID('tempdb..#modfiles', N'U') is not null
  drop table #modfiles

select modify_file 
into #modfiles
from #dbfiles 
where 
  modify_file is not null 
  and type_desc <> 'LOG'  -- exclude log file
order by size_mb asc

while exists (select top 1 * from #modfiles) begin
  select top 1 @cmd = modify_file from #modfiles
  delete #modfiles where modify_file = @cmd
  if @debug = 1
    print @cmd
  else
    exec sp_executesql @cmd
end

if @debug = 1
  select * from #dbfiles
else
  insert into DBA.dbo.DbFileGrowth
  select *, GETDATE() from #dbfiles

return



