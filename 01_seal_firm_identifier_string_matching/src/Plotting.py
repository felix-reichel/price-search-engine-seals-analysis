from matplotlib import pyplot as plt
from static import FileHelper


class Plotting:
    @staticmethod
    def plot_nchars_distribution(filtered_retailers_df):
        """Plot histogram of character lengths distribution."""
        nchars_distribution = [len(name) for name in filtered_retailers_df[1].values]
        plt.figure(figsize=(8, 6))
        plt.hist(nchars_distribution, bins=range(min(nchars_distribution), max(nchars_distribution) + 1),
                 edgecolor='black')
        plt.xlabel('Character Length')
        plt.ylabel('Frequency')
        plt.title('Distribution of Character Lengths (nchars)')
        plt.grid(True)
        plt.show()

    @staticmethod
    def plot_matched_retailers_suffixes_distribution():
        """Plot distribution of retailers by suffix."""
        matched_retailers_df = FileHelper.read_csv("matched_retailers_set.csv")
        suffix_counts = matched_retailers_df[1].str.extract(r'-(\w+)$')[0].value_counts()
        suffix_counts.plot(kind='bar', color='skyblue')
        plt.title('Distribution of Retailers by Suffix')
        plt.xlabel('Suffix')
        plt.ylabel('Count')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
