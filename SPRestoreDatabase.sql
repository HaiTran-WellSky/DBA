USE master;
GO

IF OBJECT_ID('SPRestoreDatabase', N'P') IS NOT NULL
  DROP PROCEDURE SPRestoreDatabase;
GO

/**************************************************************************************************
EXEC SPRestoreDatabase 
  @sourcedb = 'ADLMaster',
  @destinationdb = 'ADLMasterDevelop',
  @FullbackupPath = '\\10.10.9.28\d$\dbBackups\FullBackup\SQL01',
  @DiffbackupPath = '\\10.10.9.28\d$\dbBackups\DiffBackup\SQL01',
  @DBdataPath = 'S:\UserDB',
  @DBlogPath = 'S:\UserDB',
  @debug = 1
***************************************************************************************************/
CREATE PROCEDURE SPRestoreDatabase
  @sourcedb sysname,
  @destinationdb sysname,
  @FullbackupPath nvarchar(512),
  @DiffbackupPath nvarchar(52),
  @DBdataPath nvarchar(512),
  @DBlogPath nvarchar(512),
  @debug bit = 0
AS

SET NOCOUNT ON

DECLARE
  @FullbackupFile varchar(255), 
  @DiffbackupFile varchar(255),
  @FullRestoreCmd nvarchar(2000),
  @DiffRestoreCmd nvarchar(2000),
  @cmd nvarchar(2000),
  @DataPath nvarchar(2000),
  @LogPath nvarchar(2000),
  @dbexists bit = 1

SET @DataPath = @DBlogPath + '\' + @destinationdb
SET @LogPath = @DBlogPath + '\' + @destinationdb

IF NOT EXISTS (SELECT * FROM sys.databases where name = @destinationdb) 
  SELECT @dbexists = 0

-- get the full and the latest diff backup file names
DECLARE @files table (
  id int identity(1,1), 
  bkfile varchar(255),
  depth int,
  isFile bit
)

SET @cmd = 'EXEC master.sys.xp_dirtree ''' + @FullbackupPath + '\' + @sourcedb + ''',0,1;'
INSERT INTO @files 
EXEC (@cmd)
SELECT @FullbackupFile = bkfile FROM @files WHERE id in (SELECT max(id) FROM @files WHERE isFile = 1)

DELETE @files
SET @cmd = 'EXEC master.sys.xp_dirtree ''' + @DiffbackupPath + '\' + @sourcedb + ''',0,1;'
INSERT INTO @files 
EXEC (@cmd)
SELECT @DiffbackupFile = bkfile FROM @files WHERE id in (SELECT max(id) FROM @files WHERE isFile = 1)

-- get LSN of Full and Diff files
DECLARE @bkheader TABLE (
  BackupName	nvarchar(128)
  ,BackupDescription	nvarchar(255)
  ,BackupType	smallint
  ,ExpirationDate	datetime
  ,Compressed	bit
  ,Position	smallint
  ,DeviceType	tinyint
  ,UserName	nvarchar(128)
  ,ServerName	nvarchar(128)
  ,DatabaseName	nvarchar(128)
  ,DatabaseVersion	int
  ,DatabaseCreationDate	datetime
  ,BackupSize	numeric(20,0)
  ,FirstLSN	numeric(25,0)
  ,LastLSN	numeric(25,0)
  ,CheckpointLSN	numeric(25,0)
  ,DatabaseBackupLSN	numeric(25,0)
  ,BackupStartDate	datetime
  ,BackupFinishDate	datetime
  ,SortOrder	smallint
  ,CodePage	smallint
  ,UnicodeLocaleId	int
  ,UnicodeComparisonStyle	int
  ,CompatibilityLevel	tinyint
  ,SoftwareVendorId	int
  ,SoftwareVersionMajor	int
  ,SoftwareVersionMinor	int
  ,SoftwareVersionBuild	int
  ,MachineName	nvarchar(128)
  ,Flags 	int
  ,BindingID	uniqueidentifier
  ,RecoveryForkID	uniqueidentifier
  ,Collation	nvarchar(128)
  ,FamilyGUID	uniqueidentifier
  ,HasBulkLoggedData	bit
  ,IsSnapshot	bit
  ,IsReadOnly	bit
  ,IsSingleUser	bit
  ,HasBackupChecksums	bit
  ,IsDamaged	bit
  ,BeginsLogChain	bit
  ,HasIncompleteMetaData	bit
  ,IsForceOffline	bit
  ,IsCopyOnly	bit	
  ,FirstRecoveryForkID	uniqueidentifier
  ,ForkPointLSN	numeric(25,0) NULL
  ,RecoveryModel	nvarchar(60)
  ,DifferentialBaseLSN	numeric(25,0) NULL
  ,DifferentialBaseGUID	uniqueidentifier
  ,BackupTypeDescription	nvarchar(60)
  ,BackupSetGUID	uniqueidentifier NULL
  ,CompressedBackupSize	bigint
)
SELECT @cmd = 'restore headeronly from disk = ''' + @FullbackupPath + '\' + @sourcedb +  '\' + @FullbackupFile + '''' 
INSERT INTO @bkheader
EXEC (@cmd)

SELECT @cmd = 'restore headeronly from disk = ''' + @DiffbackupPath + '\' + @sourcedb +  '\' + @DiffbackupFile + '''' 
INSERT INTO @bkheader
EXEC (@cmd)

-- get db's logical and physical name from bk file
DECLARE @filelist TABLE (
  LogicalName nvarchar(128),
  PhysicalName nvarchar(260),
  Type char(1),
  FileGroupName nvarchar(128),
  Size numeric(20,0),
  MaxSize numeric(20,0),
  FileId bigint,
  CreateLSN numeric(25,0),
  DropLSN numeric(25,0),
  UniqueID binary,
  ReadOnlyLSN numeric(25,0),
  ReadWriteLSN numeric(25,0),
  BackupSizeInBytes bigint,
  SourceBlockSize int,
  FileGroupID int,
  LogGroupGUID uniqueidentifier,
  DifferentialBaseLSN numeric(25,0),
  DifferentialBaseGUID uniqueidentifier,
  IsReadOnly bit,
  IsPresent bit,
  TDEThumbprint varbinary(32)
)

SELECT @cmd = 'restore filelistonly from disk = ''' + @FullbackupPath + '\' + @sourcedb +  '\' + @FullbackupFile + '''' 
INSERT INTO @filelist
EXEC (@cmd)

-- create restore FULL statement
SELECT @FullRestoreCmd = 'RESTORE DATABASE [' + @destinationdb + ']' + char(10) + 
  'FROM DISK = ''' + @FullbackupPath + '\' + @sourcedb +  '\' + @FullbackupFile + '''' + char(10) + 
  'WITH NORECOVERY, FILE = 1'

SELECT @FullRestoreCmd = @FullRestoreCmd + char(10) +
  CASE [Type]
    WHEN 'D' THEN '  ,MOVE ''' + LogicalName + ''' TO ''' + @DBdataPath + '\' + @destinationdb + '\' + right(PhysicalName, charindex('\', reverse(PhysicalName))-1) + ''''
	WHEN 'L' THEN '  ,MOVE ''' + LogicalName + ''' TO ''' + @DBlogPath + '\' + @destinationdb + '\' + right(PhysicalName, charindex('\', reverse(PhysicalName))-1) + ''''
  END
FROM @filelist

IF @dbexists = 1
  SELECT @FullRestoreCmd = @FullRestoreCmd + char(10) + ',REPLACE'

-- create restore DIFF statement (if LSN of Diff doesn't match with LSN of Full then skip restore Diff)
IF (SELECT CheckpointLSN FROM @bkheader WHERE BackupTypeDescription = 'Database') = (SELECT DatabaseBackupLSN FROM @bkheader WHERE BackupTypeDescription = 'Database Differential')
  SELECT @DiffRestoreCmd = 'RESTORE DATABASE [' + @destinationdb + ']' + char(10) + 'FROM DISK = ''' + @DiffbackupPath + + '\' + @sourcedb +  '\' + + @DiffbackupFile + '''' + char(10) + 'WITH RECOVERY;'
ELSE  -- do not need to restore Diff
  SELECT @DiffRestoreCmd = 'RESTORE DATABASE [' + @destinationdb + ']' + char(10) + 'WITH RECOVERY;'

IF @debug = 1 BEGIN
  PRINT '-------------------------------------------------'
  PRINT @FullRestoreCmd
  PRINT char(10) + @DiffRestoreCmd

END
ELSE BEGIN
  -- set db to single_user mode
  SET @cmd = 'IF DB_ID(''' + @destinationdb + ''') IS NOT NULL AND DATABASEPROPERTYEX(''' + @destinationdb + ''', ''status'') = ''ONLINE''' + char(10) +
    'ALTER DATABASE [' + @destinationdb + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;'
  EXEC (@cmd)

  -- create sub dir
  EXEC master.dbo.xp_create_subdir @DataPath
  EXEC master.dbo.xp_create_subdir @LogPath

  -- run restore db
  PRINT 'Restoring ... [' + @destinationdb + ']...'
  EXEC (@FullRestoreCmd)
  EXEC (@DiffRestoreCmd)

  -- reset db to multi_user mode
  SET @cmd = 'IF DB_ID(''' + @destinationdb + ''') IS NOT NULL AND DATABASEPROPERTYEX(''' + @destinationdb + ''', ''status'') = ''ONLINE''' + char(10) +
    'ALTER DATABASE [' + @destinationdb + '] SET MULTI_USER WITH ROLLBACK IMMEDIATE;'
  EXEC (@cmd)
END



