import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm

filtered_haendler_bez = pd.read_csv('../data/filtered_haendler_bez.csv', sep=';', header=None)
filtered_haendler_bez.columns = ['index', 'haendler_bez']

df = pd.read_parquet('../data/haendler.parquet')
df = df[df['haendler_bez'].isin(filtered_haendler_bez['haendler_bez'])]

liefert_at_counts = df['liefert_at'].value_counts(dropna=False)
liefert_de_counts = df['liefert_de'].value_counts(dropna=False)

fig, axes = plt.subplots(1, 2, figsize=(12, 6))

axes[0].bar(liefert_at_counts.index.astype(str), liefert_at_counts.values, color='blue')
axes[0].set_title('liefert_at Value Counts')
axes[0].set_xlabel('Value')
axes[0].set_ylabel('Count')

axes[1].bar(liefert_de_counts.index.astype(str), liefert_de_counts.values, color='green')
axes[1].set_title('liefert_de Value Counts')
axes[1].set_xlabel('Value')
axes[1].set_ylabel('Count')

plt.tight_layout()
plt.show()


# VIA A PARQUET VIEWER QUERY:

# Min dtimebegin    WHERE liefert_at IS NOT NULL
# = 1434253021  >= 2015, 983 Ergebnisse

# Min dtimebegin    WHERE liefert_de IS NOT NULL
# = 1434253021  >= 2015, 931 Ergebnisse

# Min dtimebegine   WHERE liefert_at IS NULL
# = 1179027421  >= 2006, nur 17 Ergebnisse

# Min dtimebegine   WHERE liefert_de IS NULL
# = 1179027421  >= 2006, nur 69 Ergebnisse

# Count   WHERE (liefert_at IS NULL OR liefert_de IS NULL) and dtimebegin < 1434253021
# 15 Ergebnisse

def get_first_last_appearance(df, variable, dtimebegin_col='dtimebegin', dtimeend_col='dtimeend'):
    subset = df.dropna(subset=[variable])

    min_time_begin = subset[dtimebegin_col].min()
    min_time_end = subset[dtimeend_col].min()
    first_appearance = min(min_time_begin, min_time_end)

    max_time_begin = subset[dtimebegin_col].max()
    max_time_end = subset[dtimeend_col].max()
    last_appearance = max(max_time_begin, max_time_end)

    return first_appearance, last_appearance


def plot_pdf_for_variable(df, variable, dtimebegin_col='dtimebegin', dtimeend_col='dtimeend'):
    unique_values = df[variable].unique()

    # unique_values = np.append(unique_values, None)  # Ensure None is included in the unique values

    min_time = df[[dtimebegin_col, dtimeend_col]].min().min()
    max_time = df[[dtimebegin_col, dtimeend_col]].max().max()

    # Adjust the step size to keep the array size manageable
    total_seconds = max_time - min_time
    step_size = max(2, total_seconds // 1000)  # Ensure at most 10,00 steps

    time_range = np.arange(min_time, max_time + 1, step_size)

    for value in unique_values:
        if pd.isnull(value):
            subset = df[df[variable].isnull()]
        else:
            subset = df[df[variable] == value]

        counts = np.zeros_like(time_range, dtype=int)

        for _, row in tqdm(subset.iterrows(), total=subset.shape[0], desc=f'Processing {variable} = {value}'):
            begin = row[dtimebegin_col] if pd.notnull(row[dtimebegin_col]) else min_time
            end = row[dtimeend_col] if pd.notnull(row[dtimeend_col]) else max_time

            begin_idx = np.searchsorted(time_range, begin, side='left')
            end_idx = np.searchsorted(time_range, end, side='right')

            counts[begin_idx:end_idx] += 1

        plt.plot(time_range, counts, label=f'{variable} = {value}')

    plt.title(f'Sample (n=filtered_gz_haendler=18k+) Count(PDF) of {variable}')
    plt.xlabel('Unix Time')
    plt.ylabel('Count')
    plt.legend()
    plt.savefig(f'PDF_of_{variable}')
    plt.close()


first_liefert_at, last_liefert_at = get_first_last_appearance(df, 'liefert_at')
first_liefert_de, last_liefert_de = get_first_last_appearance(df, 'liefert_de')

print(f'First and last appearance of Non-Null liefert_at: {first_liefert_at}, {last_liefert_at}')
print(f'First and last appearance of Non-Null liefert_de: {first_liefert_de}, {last_liefert_de}')
# Für Null werte > 2006
# Ohne Null Werte (erste nicht Null werte von) liefert_at und liefert_de erst ab > 2015

# draw sample as test
#df_sample = df.sample(n=10000, random_state=42)

plot_pdf_for_variable(df, 'liefert_at')
plot_pdf_for_variable(df, 'liefert_de')
