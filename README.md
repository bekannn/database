# database optimization with parallelism and concurrency
Language: Python
Database: Postgresql
Database Table: Customers, Products, Orders, Financial Transactions
1. Basic CRUD operation:
- Generate fake data to be inserted using Faker in Python
  Inserting 10,000 elements into the database:
  - Sequential insertion: 450 seconds
  - Concurrency using ThreadPoolExecutor: 120 seconds
  - Sequential insertion with database connection pool: 10 seconds
  - Concurrency with database connection pool: 3 seconds
  - Partitioning data (batch) and concurrency with database connection pool: 0.4 seconds
2. Indexing:
- Creat B-tree index and experiment with different types of it, due to database size, using B-Tree index can already opitimize query execution time.
3. Database Aggregation (SUM,AVG,VARIANCE function):
- Implement aggregation function and executed some queries:
  ex. value aggregation of the amount each customer paid in financial transactions table (9000 rows):
  - Basic Aggregation function: 0.07 seconds
    -> Using a single database connection & executes the query once
  - Concurrency(ThreadPoolExecutor) with partitioning data: 0.18 seconds
    -> Partitioning data into samller chunks
    -> Slower performance due to overheads from many parallel database connections in the pool
    -> Parallelism does not succeed over basic function this time since the data table size is not significantly large enough.

Limitation: Lack of former formal database knowledge. Therefore, affecting time mangement since the time taken for the project was spent a lot to the database part rather than the parallelism.

Further Optimization:
- Develop parallel aggregation function to succeed over basic execution
- More experiment and analysis with database parallel setting variation (Changing PostgresQL parallelism default setting & Adding more data into tables for larger table size)
- Explore more into aggregations with more complex queries
- Develop further financial analysis with concurrency and parallelism utilization in order to make the database more practical and efficient (e.g. Revenue trend)

Benefit:
- Develop understanding on database structure and processes
- Gain more insight about parallelism and concurrency in Python
- Develop an idea to implement more projects outside the class in the future since it provides one of the ways to gain real-world experience and develop concrete coding skill

  
