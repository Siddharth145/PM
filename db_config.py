"""
Database config file for KPI Form application.

Fill in your MySQL credentials here. This file is intentionally separate so
you can keep credentials out of the main application file. Do NOT commit
real passwords to public repos. Use this file only for local development
or in secure private repositories.

Example:
    MYSQL_HOST = '127.0.0.1'
    MYSQL_PORT = 3306
    MYSQL_USER = 'kpi_user'
    MYSQL_PASSWORD = 'supersecret'
    MYSQL_DATABASE = 'kpi_db'

If you prefer environment variables, leave values as None.
"""

# Default placeholders (edit these values for your environment)
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'Ojasvi@SQL123'
MYSQL_DATABASE = 'engrc_2_internaluat'

## Optional: set a flag to prevent accidental migrations in sensitive envs
DISABLE_STARTUP_MIGRATION = False
