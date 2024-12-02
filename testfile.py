import chardet

import pandas as pd

file_2023 = pd.read_csv('NBA-stats_2023.csv')
file_2024 = pd.read_csv('NBA-stats_2024.csv')

print("Columns in 2023 file:", file_2023.columns)
print("Columns in 2024 file:", file_2024.columns)

print("First few rows of 2023 file:", file_2023.head())
print("First few rows of 2024 file:", file_2024.head())

print("Summary of 2023 file:")
print(file_2023.info())
print("Summary of 2024 file:")
print(file_2024.info())


with open('NBA-stats_2023.csv', 'rb') as file:
    print(chardet.detect(file.read(1024)))
with open('NBA-stats_2024.csv', 'rb') as file:
    print(chardet.detect(file.read(1024)))