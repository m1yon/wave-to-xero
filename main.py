from datetime import timedelta

import polars as pl


def main():
    xero_df = pl.read_csv("xero-template.csv")
    wave_df = pl.read_csv("wave.ignore.csv")
    print(xero_df.columns)

    with pl.Config(tbl_cols=-1):
        print(wave_df)

    result_df = wave_df.select(
        pl.col("Customer").alias("*ContactName"),
        pl.col("Invoice Number").alias("*InvoiceNumber"),
        pl.col("Transaction Date")
        .str.strptime(pl.Date, format="%m/%d/%y")
        .alias("*InvoiceDate"),
        (
            pl.col("Transaction Date").str.strptime(pl.Date, format="%m/%d/%y")
            + timedelta(days=30)
        ).alias("*InvoiceDueDate"),
    )
    print(result_df)


if __name__ == "__main__":
    main()
