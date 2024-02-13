import dlt
import duckdb

######## QUESTION 1 ########
def square_root_generator(limit):
    for i in range(1, limit+1):
        yield i ** 0.5

print(
    "Question 1:",
    f"{sum(list(square_root_generator(5))):.3f}"
)
############################

######## QUESTION 2 ########
for item in square_root_generator(13):
    pass
print("Question 2:", f"{item:.3f}")
############################

######## QUESTION 3 ########
def people_1():
    for i in range(1, 6):
        yield {"ID": i,
               "Name": f"Person_{i}",
               "Age": 25 + i,
               "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i,
               "Name": f"Person_{i}",
               "Age": 30 + i,
               "City": "City_B",
               "Occupation": f"Job_{i}"
               }

pipeline = dlt.pipeline(pipeline_name="pipeline",
                        destination='duckdb',
                        dataset_name='dlt_homework')

pipeline.run(people_1(), table_name="people")

pipeline.run(people_2(), table_name="people", write_disposition="append")

conn = duckdb.connect("pipeline.duckdb")

print("Question 3:\n"
      f"{conn.sql('SELECT SUM(age) FROM dlt_homework.people')}")
############################

######## QUESTION 4 ########
# Drop table for question 4
conn.sql("DROP TABLE dlt_homework.people")

pipeline.run(people_1(),
             table_name="people",
             write_disposition="merge",
             primary_key="id")

pipeline.run(people_2(),
             table_name="people",
             write_disposition="merge",
             primary_key="id")

print("Question 4:\n"
      f"{conn.sql('SELECT SUM(age) FROM dlt_homework.people')}")


conn.sql("DROP SCHEMA dlt_homework CASCADE")
conn.close()