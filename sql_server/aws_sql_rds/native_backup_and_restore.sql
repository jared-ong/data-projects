-- References: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/SQLServer.Procedural.Importing.html

-- Set backup compression on
exec rdsadmin..rds_set_configuration 'S3 backup compression', 'true'; 

-- Restore database
-- An Amazon S3 bucket to store your backup files.
-- An AWS Identity and Access Management (IAM) role to access the bucket.
-- The SQLSERVER_BACKUP_RESTORE option added to an option group on your DB instance.
exec msdb.dbo.rds_restore_database 
     @restore_db_name = 'parse_ddl', 
     @S3_arn_to_restore_from = 'arn:aws:s3:::jong-aws-bucket/parse_ddl_2018-10-22.bak'

-- Backup database to S3
exec msdb.dbo.rds_backup_database 
        @source_db_name='parse_ddl', 
        @s3_arn_to_backup_to='arn:aws:s3:::jong-aws-bucket/parse_ddl_test.bak',
        @overwrite_S3_backup_file=1,
        @type='full';

-- Check the status of the restore/backup task
exec msdb.dbo.rds_task_status @db_name='parse_ddl';

-- Cancel the task
exec msdb.dbo.rds_cancel_task @task_id=1234;

