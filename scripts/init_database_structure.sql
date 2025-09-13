/*
 * Database Creation Script for DataWarehouse
 * This script creates a new data warehouse database with bronze, silver, and gold schemas
 * Includes configuration options and best practices for data warehouse setup
 */

SET NOCOUNT ON;
SET XACT_ABORT ON; -- Automatically rollback on error

BEGIN TRY
    DECLARE @StartTime DATETIME2 = SYSDATETIME();
    DECLARE @Message NVARCHAR(4000);
    DECLARE @DatabaseName NVARCHAR(128) = N'DataWarehouse';
    DECLARE @DataPath NVARCHAR(260);
    DECLARE @LogPath NVARCHAR(260);
    DECLARE @RecoveryModel NVARCHAR(60) = N'SIMPLE'; -- Consider BULK_LOGGED for ETL operations
    DECLARE @PageVerifyOption NVARCHAR(60) = N'CHECKSUM';
    DECLARE @CompatibilityLevel INT = 150; -- SQL Server 2019

    -- Get default file paths
    SELECT 
        @DataPath = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(260)),
        @LogPath = CAST(SERVERPROPERTY('InstanceDefaultLogPath') AS NVARCHAR(260));

    -- Log script execution start
    SET @Message = CONCAT('Starting database creation for: ', @DatabaseName, ' at: ', CONVERT(NVARCHAR, @StartTime, 120));
    RAISERROR(@Message, 0, 1) WITH NOWAIT;

    -- Check if database exists and drop it if it does
    IF DB_ID(@DatabaseName) IS NOT NULL
    BEGIN
        SET @Message = CONCAT('Dropping existing database: ', @DatabaseName);
        RAISERROR(@Message, 0, 1) WITH NOWAIT;
        
        -- Set database to single user mode with rollback immediate
        EXEC('ALTER DATABASE [' + @DatabaseName + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;');
        
        -- Drop the database
        EXEC('DROP DATABASE [' + @DatabaseName + '];');
        
        SET @Message = CONCAT('Database dropped: ', @DatabaseName);
        RAISERROR(@Message, 0, 1) WITH NOWAIT;
    END

    -- Create the database with optimized settings for data warehousing
    SET @Message = CONCAT('Creating database: ', @DatabaseName);
    RAISERROR(@Message, 0, 1) WITH NOWAIT;

    DECLARE @SQL NVARCHAR(MAX) = CONCAT(
        'CREATE DATABASE [', @DatabaseName, '] ',
        'CONTAINMENT = NONE ',
        'ON PRIMARY ',
        '(NAME = N''', @DatabaseName, '_Data'', ',
        'FILENAME = N''', @DataPath, @DatabaseName, '.mdf'', ',
        'SIZE = 512MB, ',
        'FILEGROWTH = 256MB, ',
        'MAXSIZE = UNLIMITED) ',
        'LOG ON ',
        '(NAME = N''', @DatabaseName, '_Log'', ',
        'FILENAME = N''', @LogPath, @DatabaseName, '_Log.ldf'', ',
        'SIZE = 256MB, ',
        'FILEGROWTH = 128MB, ',
        'MAXSIZE = 2GB);' -- Consider larger log size for ETL operations
    );

    EXEC sp_executesql @SQL;

    -- Configure database settings
    EXEC('ALTER DATABASE [' + @DatabaseName + '] SET RECOVERY ' + @RecoveryModel + ';');
    EXEC('ALTER DATABASE [' + @DatabaseName + '] SET PAGE_VERIFY ' + @PageVerifyOption + ';');
    EXEC('ALTER DATABASE [' + @DatabaseName + '] SET COMPATIBILITY_LEVEL = ' + CAST(@CompatibilityLevel AS NVARCHAR(3)) + ';');
    EXEC('ALTER DATABASE [' + @DatabaseName + '] SET AUTO_CREATE_STATISTICS ON;');
    EXEC('ALTER DATABASE [' + @DatabaseName + '] SET AUTO_UPDATE_STATISTICS ON;');
    EXEC('ALTER DATABASE [' + @DatabaseName + '] SET AUTO_UPDATE_STATISTICS_ASYNC ON;');
    EXEC('ALTER DATABASE [' + @DatabaseName + '] SET QUERY_STORE = ON;'); -- Enable Query Store for performance monitoring

    SET @Message = CONCAT('Database created: ', @DatabaseName);
    RAISERROR(@Message, 0, 1) WITH NOWAIT;

    -- Switch to the new database
    USE [DataWarehouse];
    GO

    -- Create schemas for data warehouse layers
    SET @Message = 'Creating data warehouse schemas...';
    RAISERROR(@Message, 0, 1) WITH NOWAIT;

    -- Bronze layer (raw/landing zone)
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    BEGIN
        EXEC('CREATE SCHEMA bronze;');
        SET @Message = 'Schema created: bronze';
        RAISERROR(@Message, 0, 1) WITH NOWAIT;
    END

    -- Silver layer (cleaned/conformed data)
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    BEGIN
        EXEC('CREATE SCHEMA silver;');
        SET @Message = 'Schema created: silver';
        RAISERROR(@Message, 0, 1) WITH NOWAIT;
    END

    -- Gold layer (business-level aggregates)
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    BEGIN
        EXEC('CREATE SCHEMA gold;');
        SET @Message = 'Schema created: gold';
        RAISERROR(@Message, 0, 1) WITH NOWAIT;
    END

    -- Create utility schema for ETL procedures and functions
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    BEGIN
        EXEC('CREATE SCHEMA etl;');
        SET @Message = 'Schema created: etl';
        RAISERROR(@Message, 0, 1) WITH NOWAIT;
    END

    -- Create audit schema for logging and monitoring
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
    BEGIN
        EXEC('CREATE SCHEMA audit;');
        SET @Message = 'Schema created: audit';
        RAISERROR(@Message, 0, 1) WITH NOWAIT;
    END

    -- Log script completion
    DECLARE @EndTime DATETIME2 = SYSDATETIME();
    DECLARE @DurationMs INT = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
    
    SET @Message = CONCAT(
        'Database creation completed successfully. ', 
        'Duration: ', @DurationMs, ' ms'
    );
    RAISERROR(@Message, 0, 1) WITH NOWAIT;

    -- Print summary
    PRINT '========================================';
    PRINT 'DATA WAREHOUSE SETUP COMPLETE';
    PRINT '========================================';
    PRINT 'Database: ' + @DatabaseName;
    PRINT 'Recovery Model: ' + @RecoveryModel;
    PRINT 'Compatibility Level: ' + CAST(@CompatibilityLevel AS NVARCHAR(3));
    PRINT 'Schemas Created: bronze, silver, gold, etl, audit';
    PRINT '========================================';

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();
    DECLARE @ErrorProcedure NVARCHAR(200) = ERROR_PROCEDURE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    
    SET @Message = CONCAT(
        'Database creation failed with error: ', @ErrorMessage,
        ' Procedure: ', ISNULL(@ErrorProcedure, 'N/A'),
        ' Line: ', @ErrorLine,
        ' Time: ', CONVERT(NVARCHAR, SYSDATETIME(), 120)
    );
    
    RAISERROR(@Message, @ErrorSeverity, @ErrorState);
    
    -- Re-throw the error to ensure the script fails
    THROW;
END CATCH
GO

-- Additional recommendations (commented out)
/*
-- Consider adding filegroups for partitioning
ALTER DATABASE [DataWarehouse] ADD FILEGROUP [FG_Current];
ALTER DATABASE [DataWarehouse] ADD FILEGROUP [FG_History];
ALTER DATABASE [DataWarehouse] ADD FILEGROUP [FG_Indexes];

-- Consider creating database roles for security
CREATE ROLE [etl_processor];
CREATE ROLE [data_analyst];
CREATE ROLE [report_consumer];
*/