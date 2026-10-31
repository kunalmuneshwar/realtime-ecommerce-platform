
import getpass
import psycopg2

try:
    password = getpass.getpass("Enter PostgreSQL password: ")

    connection = psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecommerce_analytics",
        user="postgres",
        password=password
    )

    print("Successfully connected to PostgreSQL!")

    cursor = connection.cursor()
    cursor.execute("SELECT current_database(), version();")

    result = cursor.fetchone()

    print("Database:", result[0])
    print("PostgreSQL:", result[1].split(",")[0])

    cursor.close()
    connection.close()

except psycopg2.Error as error:
    print("Database connection failed:", error)
