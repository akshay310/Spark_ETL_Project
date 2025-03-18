This folder contains some log files which show the debugging and logging

To activate PostgreSQL
1. Open WSL-UBUNTU and create postgresql and records database for the project
2. Run sudo service postgresql start
3. Run sudo service postgresql status
4. Run psql -U postgres
5. Enter password when prompted
6. We enter postgres bash
7. Run \c records;
8. Run \dt;
9. Run SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
10. Run \d iowa_liquor_sales;
11. Run SELECT * FROM iowa_liquor_sales LIMIT 10;

