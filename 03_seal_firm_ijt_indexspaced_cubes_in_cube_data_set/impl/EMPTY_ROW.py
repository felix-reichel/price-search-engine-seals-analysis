# is able to append an empty row to a pl.DataFrame
# each column's default value is null
import polars as pl

cols = [
    "haendler_j",
    "produkt_id_i",
    "zeit_t",

    "siegel_ehi_jt",
    "siegel_ecom",
    "siegel_hv",

    "datum_t",
    "Unixtime_t",

    # Add new Helpers-Vars here
    #

    ###

    #
    "missing _data_week_t",
    "missing _data_day_t",

    #

    # Stammdaten
    "is_at_jt",
    "is_de_jt",
    "is_other_jt",
    "liefert_at_jt",
    "liefert_de_jt",
    "liefert_other_jt",
    "m_liefert_jt",
    "laden_jt",
    "abhol_jt",

    "yshop_jt",
    "yversand,jt",
    "ylagerbestand_jt",
    "ybestellstatus_jt",
    "ykundenservice_jt",
    "ym_shop_jt",
    "ym_versand_jt",
    "ym_lagerbestand_jt",
    "ym_bestellstatus_jt",
    "ym_kundenservice_jt",
    "ygesamtbewertung_jt",
    "ym_gesamtbewertung_jt",

    "firm_assortment_jt",
    "firm_specialization_jt",
    "sum_clicks_jt",
    "firm_birth_j",
    "Produkt_bez_i",
    "subsubkat_i",
    "subkat_i",
    "kat_i",
    "Top200rang_i",

    "preis_ijt",
    "avail_ijt",
    "price_ranking_ijt",
    "price_ranking_de_ijt",
    "price_ranking_at_ijt",
    "bestpreis_it",
    "bestpreis_de_it",
    "bestpreis_at_it",

    "vfb_in_at_ijt",
    "vfb_in_de_ijt",
    "vfb_in_others_ijt"
    "m_vfb_ijt",

    "vk_vork_at_ijt",
    "vk_vork_de_ijt",
    "vk_nachn_at_ijt",
    "vk_nachn_de_ijt",
    "m_vk_var",

    "clicks_ijt",
    "clicks_de_ijt",
    "clicks_at_ijt",

    "lct_produkt_ijt",
    "lct_subsubkat_ijt",
    "lct_subcat_ijt",
    "lct_cat_ijt",
    "lct_produkt_de_ijt",
    "lct_subsubkat_de_ijt",
    "lct_subcat_de_ijt",
    "lct_cat_de_ijt",
    "lct_produkt__at_ijt",
    "lct_subsubkat__at_ijt",
    "lct_subcat_at__ijt",
    "lct_cat_at_ijt",

    "share_clicks_ijt",

    "share_lct_produkt_ijt",
    "share_lct_subsubkat_ijt",
    "share_clicks_de_ijt",
    "share_lct_produkt__de_ijt",
    "share_lct_subsubkat__de_ijt",
    "share_clicks__at_ijt",
    "share_lct_produkt__at_ijt",  # country-variant
    "share_lct_subsubkat__at_ijt",

    # IVs
    "Instrument1_ijt",
    "Instrument2_ijt",
    "Instrument3_ijt",
    "Instrument4_ijt"

    # Introduce new Vars
]

error_row_polars = {col: "CalculationError" for col in cols}
empty_df_polars = pl.DataFrame([error_row_polars])
print(empty_df_polars.width)  # 84


class ColumnMismatchError(Exception):
    def __init__(self, message="Column count does not match between the DataFrame and the empty row."):
        self.message = message
        super().__init__(self.message)


def append_empty_row(df: pl.DataFrame) -> pl.DataFrame:
    """
    Appends an empty row to the given Polars DataFrame where all columns are set to `null`.
    Throws a ColumnMismatchError if the number of columns in the DataFrame does not match the width
    of the empty row to be appended.

    Parameters:
    -----------
    df : pl.DataFrame
        The DataFrame to which an empty row is to be appended.

    Returns:
    --------
    pl.DataFrame
        A new DataFrame with the empty row appended.

    Raises:
    -------
    ColumnMismatchError:
        If the number of columns in the DataFrame does not match the number of columns in the empty row.
    """
    if df.width != empty_df_polars.width:
        raise ColumnMismatchError(
            f"DataFrame has {df.width} columns, but the empty row has {empty_df_polars.width} columns."
        )

    return df.vstack(empty_df_polars)
