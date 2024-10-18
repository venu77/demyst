import pandas as pd
import csv
from faker import Faker

fake = Faker()

input_file = 'C:\Venu\PERSONAL\MATERIALS\DEMYST\sample_names.csv'
output_file = 'C:\Venu\PERSONAL\MATERIALS\DEMYST\msak_names.csv'
print(input_file)
print(output_file)


# Define the chunk size (number of rows to process at a time)
chunk_size = 100  # Adjust based on your laptop's memory capacity


# Function to anonymize specific columns
def anonymize_data(chunk):
    chunk['first_name'] = chunk['first_name'].apply(lambda x: fake.first_name())
    chunk['last_name'] = chunk['last_name'].apply(lambda x: fake.last_name())
    chunk['address'] = chunk['address'].apply(lambda x: fake.address())
    return chunk


# Process the CSV file in chunks
with pd.read_csv(input_file, delimiter = ',',  chunksize=chunk_size) as reader:
    for i, chunk in enumerate(reader):
        # Anonymize the chunk
        anonymized_chunk = anonymize_data(chunk)

        # Write the chunk to the output file (append mode after the first chunk)
        if i == 0:
            anonymized_chunk.to_csv(output_file, index=False)  # Write header in the first chunk
        else:
            anonymized_chunk.to_csv(output_file, mode='a', header=False, index=False)  # Append without header



