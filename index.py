from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['localhost'])  # Replace 'localhost' with your Cassandra container IP if needed
session = cluster.connect()

# Create keyspace and table
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sample_keyspace 
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS sample_keyspace.sample_table (
        id int PRIMARY KEY,
        name text,
        age int
    )
""")

# Insert data into the table
session.execute("""
    INSERT INTO sample_keyspace.sample_table (id, name, age)
    VALUES (%s, %s, %s)
""", (1, 'Alice', 30))

# Read data from the table
rows = session.execute("SELECT * FROM sample_keyspace.sample_table")
for row in rows:
    print(row.id, row.name, row.age)

# Update data in the table
session.execute("""
    UPDATE sample_keyspace.sample_table
    SET age = %s
    WHERE id = %s
""", (25, 1))

# Delete data from the table
session.execute("""
    DELETE FROM sample_keyspace.sample_table
    WHERE id = %s
""", (1,))

# Close the connection
cluster.shutdown()
