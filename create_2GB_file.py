import csv
from faker import Faker
import os

# Initialize the Faker object
fake = Faker()

# Define the output file path
output_file = "<File_Path>"

# Estimate number of rows to generate (for 2 GB, assume around 20 million rows)
num_rows = 20_000_000
chunk_size = 100_000  # Rows to write per chunk


# Function to generate a chunk of data
def generate_data_chunk(size):
    data = []
    for _ in range(size):
        data.append([
            fake.first_name(),
            fake.last_name(),
            fake.address().replace("\n", ", "),  # Clean the address field
            fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d')
        ])
    return data


# Create the CSV file in chunks
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write the header
    writer.writerow(["first_name", "last_name", "address", "date_of_birth"])

    # Write data in chunks
    total_written = 0
    while total_written < num_rows:
        chunk_data = generate_data_chunk(min(chunk_size, num_rows - total_written))
        writer.writerows(chunk_data)
        total_written += len(chunk_data)
        print(f"{total_written} rows written...")

output_file