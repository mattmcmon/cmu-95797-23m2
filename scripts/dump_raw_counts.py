# Module used for interacting with duckdb
import duckdb

# Creates a connection to our database
con = duckdb.connect('main.db')

# Creates a SQL query to select all tables in database
table_names = con.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES").fetchall()

# Loops through list of table names and prints one at a time
for table_name in table_names:
    # Accesses table_name field, stored in index value 2
    print(table_name[2])
    # Uses table_name from above and displays number of rows
    row_query = f"SELECT COUNT(*) FROM {table_name[2]}"
    row_count = con.sql(row_query).fetchall()
    # Flattens list to just display the number of rows
    # Removes commas, brackets, and parentheses)
    print(row_count[0][0], "\n")

# Closes connection 
con.close()

