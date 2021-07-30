"""
This is the entrypoint to the program. "python main.py" will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""
from src.some_storage_library import SomeStorageLibrary, project_root
import os

if __name__ == "__main__":
    """Entrypoint"""
    print("Beginning the ETL process...")

    output_filename = "FORMATTED_DATA.csv"

    with open(output_filename, "a") as formatted_file:
        columns_source = os.path.join(project_root, "data", "source", "SOURCECOLUMNS.txt")
        data_source = os.path.join(project_root, "data", "source", "SOURCEDATA.txt")

        with open(columns_source) as columns_file:
            position_column_name = {}

            for line in columns_file:
                position, column_name = line.split("|")
                position_column_name[int(position)] = column_name.strip()

            columns = []
            for position in sorted(position_column_name):
                columns.append(position_column_name[position])

            formatted_file.write(",".join(columns))

        with open(data_source) as data_file:
            formatted_file.write("\n")
            for line in data_file:
                formatted_file.write(",".join(line.split("|")))

    SomeStorageLibrary().load_csv(output_filename)
