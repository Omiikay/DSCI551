from pyspark.sql import SparkSession
import pyspark.sql.functions as fc

# Initialize SparkSession
spark = SparkSession.builder.appName("Employee Dataset Analysis").getOrCreate()

# Load the datasets as DataFrames
employees_df = spark.read.json('employees.json', multiLine=True)
departments_df = spark.read.json('departments.json', multiLine=True)
locations_df = spark.read.json('locations.json', multiLine=True)
projects_df = spark.read.json('projects.json', multiLine=True)
assignments_df = spark.read.json('assignments.json', multiLine=True)

def query_a(employees_df, departments_df, locations_df, projects_df, assignments_df):
    """
    Find the name and location of employees working in North America (region).
    """
    # Implement your transformation logic and return the dataframe that consists of requirements provided in the question


# %query_a output:


def query_b(employees_df, departments_df, locations_df, projects_df, assignments_df):
    """
    Find names of employees who are not managers.
    """
    # Implement your transformation logic and return the dataframe that consists of requirements provided in the question


# %query_b output:

def query_c(employees_df, departments_df, locations_df, projects_df, assignments_df):
    """
    Find the name and salary of employees who worked at the "Marketing" department and are located in "Berlin".
    Assume there is only one such department and one such location.
    """
    # Implement your transformation logic and return the dataframe that consists of requirements provided in the question


# %query_c output:

def query_d(employees_df, departments_df, locations_df, projects_df, assignments_df):
    """
    Find names of employees who worked for at least 100 hours for projects.
    Return names of employees and the total number of hours they have worked.
    Assume that no two employees have the same name.
    Order the names alphabetically (ascending).
    """
    # Implement your transformation logic and return the dataframe that consists of requirements provided in the question


# %query_d output:

def query_e(employees_df, departments_df, locations_df, projects_df, assignments_df):
    """
    Find projects which have been assigned with the largest number of employee hours.
    Note there may be multiple such projects.
    Return project names only.
    """
    # Implement your transformation logic and return the dataframe that consists of requirements provided in the question


# %query_e output:


def main():
    print("Query A:")
    query_a(employees_df, departments_df, locations_df, projects_df, assignments_df).show()

    print("Query B:")
    query_b(employees_df, departments_df, locations_df, projects_df, assignments_df).show()

    print("Query C:")
    query_c(employees_df, departments_df, locations_df, projects_df, assignments_df).show()

    print("Query D:")
    query_d(employees_df, departments_df, locations_df, projects_df, assignments_df).show()

    print("Query E:")
    query_e(employees_df, departments_df, locations_df, projects_df, assignments_df).show()


if __name__ == "__main__":
    main()
