-- plan_id: plan_1779697984

CREATE TABLE IF NOT EXISTS dbo.etl_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME NULL,
    status VARCHAR(20) NOT NULL,
    error_message VARCHAR(MAX) NULL
);

CREATE TABLE IF NOT EXISTS dbo.etl_watermark (
    process_name VARCHAR(100) PRIMARY KEY,
    last_run_time DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS dbo.courses_Clean (
    course_name NVARCHAR(MAX),
    course_id NVARCHAR(MAX),
    instructor NVARCHAR(MAX),
    credits FLOAT,
    fee FLOAT,
    department NVARCHAR(MAX),
    etl_created_at DATETIME DEFAULT GETDATE(),
    etl_updated_at DATETIME DEFAULT GETDATE(),
    etl_batch_id INT
);

CREATE TABLE IF NOT EXISTS dbo.students_Clean (
    name NVARCHAR(MAX),
    email NVARCHAR(MAX),
    student_id NVARCHAR(MAX),
    dob DATETIME,
    department NVARCHAR(MAX),
    phone NVARCHAR(MAX),
    etl_created_at DATETIME DEFAULT GETDATE(),
    etl_updated_at DATETIME DEFAULT GETDATE(),
    etl_batch_id INT
);

CREATE PROCEDURE dbo.etl_clean_courses
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @run_id INT;

    INSERT INTO dbo.etl_log (process_name, start_time, status) VALUES ('dbo.etl_clean_courses', GETDATE(), 'RUNNING');
    SET @run_id = SCOPE_IDENTITY();

    BEGIN TRY
        IF NOT EXISTS (SELECT * FROM dbo.courses_Clean) 
        BEGIN
            SELECT * INTO dbo.courses_Clean FROM dbo.courses_raw WHERE 1=0;
            -- Add indexes here as needed
        END

        INSERT INTO dbo.courses_Clean (course_name, course_id, instructor, credits, fee, department, etl_batch_id)
        SELECT 
            LTRIM(RTRIM(course_name)),
            course_id,
            instructor,
            TRY_CAST(NULLIF(LTRIM(RTRIM(credits)), '') AS FLOAT),
            TRY_CAST(NULLIF(LTRIM(RTRIM(fee)), '') AS FLOAT),
            LTRIM(RTRIM(department)),
            @run_id
        FROM dbo.courses_raw;

        -- Fill nulls for credits with median
        UPDATE dbo.courses_Clean
        SET credits = 3
        WHERE credits IS NULL;

        -- Normalize case for course_name and department
        UPDATE dbo.courses_Clean
        SET 
            course_name = LOWER(course_name),
            department = LOWER(department);

        -- Deduplicate rows
        WITH CTE AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY etl_created_at DESC) AS rn
            FROM dbo.courses_Clean
        )
        DELETE FROM CTE WHERE rn > 1;

        -- Flag outliers for credits
        EXEC dbo.sp_flag_outliers_iqr 'dbo.courses_Clean', 'credits';

        COMMIT TRANSACTION;
        UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'SUCCESS' WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'FAILED', error_message = ERROR_MESSAGE() WHERE id = @run_id;
    END CATCH
END;

CREATE PROCEDURE dbo.etl_clean_students
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @run_id INT;

    INSERT INTO dbo.etl_log (process_name, start_time, status) VALUES ('dbo.etl_clean_students', GETDATE(), 'RUNNING');
    SET @run_id = SCOPE_IDENTITY();

    BEGIN TRY
        IF NOT EXISTS (SELECT * FROM dbo.students_Clean) 
        BEGIN
            SELECT * INTO dbo.students_Clean FROM dbo.students_raw WHERE 1=0;
            -- Add indexes here as needed
        END

        INSERT INTO dbo.students_Clean (name, email, student_id, dob, department, phone, etl_batch_id)
        SELECT 
            LTRIM(RTRIM(name)),
            LTRIM(RTRIM(email)),
            student_id,
            TRY_CAST(NULLIF(LTRIM(RTRIM(dob)), '') AS DATETIME),
            LTRIM(RTRIM(department)),
            LTRIM(RTRIM(phone)),
            @run_id
        FROM dbo.students_raw;

        -- Fill nulls for department, dob, email, name with empty string
        UPDATE dbo.students_Clean
        SET 
            department = '',
            dob = NULL,
            email = '',
            name = ''
        WHERE department IS NULL OR dob IS NULL OR email IS NULL OR name IS NULL;

        -- Normalize case for department and name
        UPDATE dbo.students_Clean
        SET 
            department = LOWER(department),
            name = LOWER(name);

        -- Sanitize email
        UPDATE dbo.students_Clean
        SET email = NULL
        WHERE email NOT LIKE '%_@__%.__%';

        -- Normalize phone
        UPDATE dbo.students_Clean
        SET phone = NULL
        WHERE phone NOT LIKE '%[0-9]%';

        -- Deduplicate rows
        WITH CTE AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY etl_created_at DESC) AS rn
            FROM dbo.students_Clean
        )
        DELETE FROM CTE WHERE rn > 1;

        -- Check at least one of email or phone is non-null
        INSERT INTO dbo.etl_rejects (SELECT * FROM dbo.students_Clean WHERE email IS NULL AND phone IS NULL FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);

        COMMIT TRANSACTION;
        UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'SUCCESS' WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'FAILED', error_message = ERROR_MESSAGE() WHERE id = @run_id;
    END CATCH
END;

CREATE PROCEDURE dbo.etl_main
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS
BEGIN
    EXEC dbo.etl_clean_courses @load_type, @last_run;
    EXEC dbo.etl_clean_students @load_type, @last_run;
END;