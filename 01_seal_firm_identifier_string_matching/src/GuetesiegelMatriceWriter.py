import pandas as pd

from CONFIG import CSV_SEPARATOR


class GuetesiegelMatriceWriter:
    @staticmethod
    def write_final_guetesiegel_matrice(matched_retailers_set):
        final_matrix = pd.DataFrame(
            columns=['Retailer',
                     'ecommerce date from',
                     'ecommerce date to',
                     'ehi_bevh from',
                     'ehi_bevh eval',
                     'handesverband year',
                     'eval interval yearly'],
            index=range(len(matched_retailers_set)))
        for index, retailer in enumerate(matched_retailers_set):
            row = {
                'Retailer': retailer,
                'ecommerce date from': None,
                'ecommerce date to': None,
                'ehi_bevh from': None,
                'ehi_bevh eval': None,
                'handesverband year': None,
                'eval interval yearly': None
            }
            final_matrix.iloc[index] = row
        final_matrix.to_csv('final_matrix.csv', index=False, sep=CSV_SEPARATOR)

