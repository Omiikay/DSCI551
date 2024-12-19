from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Employee Dataset Analysis").getOrCreate()

# Load the datasets as DataFrames
employees_df = spark.read.json('employees.json', multiLine=True)
departments_df = spark.read.json('departments.json', multiLine=True)
locations_df = spark.read.json('locations.json', multiLine=True)
projects_df = spark.read.json('projects.json', multiLine=True)
assignments_df = spark.read.json('assignments.json', multiLine=True)

# Convert DataFrames to RDDs
employees_rdd = employees_df.rdd
departments_rdd = departments_df.rdd
locations_rdd = locations_df.rdd
projects_rdd = projects_df.rdd
assignments_rdd = assignments_df.rdd


def query_a(employees_rdd, departments_rdd, locations_rdd, projects_rdd, assignments_rdd):
    """
    Find the name and location of employees working in North America (region).
    """
    # Implement the RDD transformation and action here


# %query_a output:


def query_b(employees_rdd, departments_rdd, locations_rdd, projects_rdd, assignments_rdd):
    """
    Find names of employees who are not managers.
    """
    # Implement the RDD transformation and action here


# %query_b output:

def query_c(employees_rdd, departments_rdd, locations_rdd, projects_rdd, assignments_rdd):
    """
    Find the name and salary of employees who worked at the "Marketing" department and are located in "Berlin".
    Assume there is only one such department and one such location.
    """
    # Implement the RDD transformation and action here


# %query_c output:

def query_d(employees_rdd, departments_rdd, locations_rdd, projects_rdd, assignments_rdd):
    """
    Find names of employees who worked for at least 100 hours for projects.
    Return names of employees and the total number of hours they have worked.
    Assume that no two employees have the same name.
    Order the names alphabetically (ascending).
    """
    # Implement the RDD transformation and action here


# %query_d output:

def query_e(employees_rdd, departments_rdd, locations_rdd, projects_rdd, assignments_rdd):
    """
    Find projects which have been assigned with the largest number of employee hours.
    Note there may be multiple such projects.
    Return project names only.
    """
    # Implement the RDD transformation and action here


# %query_e output:


def main():
    print("Query A:")
    print(query_a(employees_rdd, departments_rdd, locations_rdd, projects_rdd, assignments_rdd).collect())

    print("Query B:")
    print(query_b(employees_rdd, departments_rdd, locations_rdd, projects_rdd, assignments_rdd).collect())

    print("Query C:")
    print(query_c(employees_rdd, departments_rdd, locations_rdd, projects_rdd, assignments_rdd).collect())

    print("Query D:")
    print(query_d(employees_rdd, departments_rdd, locations_rdd, projects_rdd, assignments_rdd).collect())

    print("Query E:")
    print(query_e(employees_rdd, departments_rdd, locations_rdd, projects_rdd, assignments_rdd).collect())


if __name__ == "__main__":
    main()
