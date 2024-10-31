import pandas as pd
xpt_file_path = '~/desktop/jared/PAQ_L.XPT'
csv_file_path = '~/desktop/jared/output.csv'

df = pd.read_sas(xpt_file_path, format='xport')

# Save it as a .csv file
df.to_csv(csv_file_path, index=False)

print("Conversion complete. CSV saved at:", csv_file_path)