"""
Configuration file for PostgreSQL connection.
"""

DB_URL = "jdbc:postgresql://localhost:5432/records"
DB_PROPERTIES = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}
