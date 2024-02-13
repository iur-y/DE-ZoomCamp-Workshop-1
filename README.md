# What is this repository?
* My answers for the questions of the first workshop of a Data Engineering course, which you can find [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)

# Setup

Remember the concept of generator? Let's practice using them to futher our understanding of how they work.

Let's define a generator and then run it as practice.

``` Python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1
```

## Question 1:
What is the sum of the outputs of the generator for limit = 5?

- **A**: 10.23433234744176
- **B**: 7.892332347441762
- **C**: 8.382332347441762
- **D**: 9.123332347441762

### Answer: `C`
### Code:
``` Python
def square_root_generator(limit):
    for i in range(1, limit+1):
        yield i ** 0.5

print(
    "Question 1:",
    f"{sum(list(square_root_generator(5))):.3f}"
)
```

## Question 2:
What is the 13th number yielded by the generator?

- **A**: 4.236551275463989
- **B**: 3.605551275463989
- **C**: 2.345551275463989
- **D**: 5.678551275463989

### Answer: `B`
### Code:
``` Python
for item in square_root_generator(13):
    pass
print("Question 2:", f"{item:.3f}")
```

## Question 3:
Below you have 2 generators. You will be tasked to load them to duckdb and answer some questions from the data.

``` Python
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
```

Load the first generator and calculate the sum of ages of all people. Make sure to only load it once.
Append the second generator to the same table as the first.
After correctly appending the data, calculate the sum of all ages of people.

- **A**: 353
- **B**: 365
- **C**: 378
- **D**: 390

### Answer: `A`
### Code:
``` Python
pipeline = dlt.pipeline(pipeline_name="pipeline",
                        destination='duckdb',
                        dataset_name='dlt_homework')

pipeline.run(people_1(), table_name="people")

pipeline.run(people_2(), table_name="people", write_disposition="append")

conn = duckdb.connect("pipeline.duckdb")

print("Question 3:\n"
      f"{conn.sql('SELECT SUM(age) FROM dlt_homework.people')}")
```

## Question 4:
Re-use the generators from Question 3.

A table's primary key needs to be created from the start, so load your data to a new table with primary key ID.

Load your first generator first, and then load the second one with merge. Since they have overlapping IDs, some of the records from the first load should be replaced by the ones from the second load.

After loading, you should have a total of 8 records, and ID 3 should have age 33.

Calculate the sum of ages of all the people loaded as described above.

- **A**: 215
- **B**: 266
- **C**: 241
- **D**: 258


### Answer: `B`
### Code:
``` Python
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
```