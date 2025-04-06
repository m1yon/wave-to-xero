from datetime import timedelta

import polars as pl


def main():
    xero_df = pl.read_csv("xero-template.csv")
    wave_df = pl.read_csv("wave.ignore.csv")
    print(xero_df.columns)

    wave_df = wave_df.filter(
        pl.col("Debit Amount (Two Column Approach)").is_null(),
        ~pl.col("Account Type").str.contains("Sales Tax"),
    )

    with pl.Config(tbl_cols=-1, tbl_rows=-1):
        print(wave_df)

    result_df = wave_df.select(
        pl.col("Customer").alias("*ContactName"),
        pl.lit(None).alias("EmailAddress"),
        pl.lit(None).alias("POAddressLine1"),
        pl.lit(None).alias("POAddressLine2"),
        pl.lit(None).alias("POAddressLine3"),
        pl.lit(None).alias("POAddressLine4"),
        pl.lit(None).alias("POCity"),
        pl.lit(None).alias("PORegion"),
        pl.lit(None).alias("POPostalCode"),
        pl.lit(None).alias("POCountry"),
        pl.col("Invoice Number").alias("*InvoiceNumber"),
        pl.lit(None).alias("Reference"),
        pl.col("Transaction Date")
        .str.strptime(pl.Date, format="%m/%d/%y")
        .alias("*InvoiceDate"),
        (
            pl.col("Transaction Date").str.strptime(pl.Date, format="%m/%d/%y")
            + timedelta(days=30)
        ).alias("*DueDate"),
        pl.lit(None).alias("InventoryItemCode"),
        pl.col("Transaction Line Description").alias("*Description"),
        pl.lit(1).alias("*Quantity"),
        (pl.col("Amount (One column)") + pl.col("Sales Tax Amount")).alias(
            "*UnitAmount"
        ),
        pl.lit(None).alias("Discount"),
        pl.when(pl.col("Account Type").str.contains("Sales Tax"))
        .then(pl.lit(2230))
        .otherwise(pl.lit(1200))
        .alias("*AccountCode"),
        pl.lit("Tax on Sales").alias("*TaxType"),
        pl.lit(None).alias("TrackingName1"),
        pl.lit(None).alias("TrackingOption1"),
        pl.lit(None).alias("TrackingName2"),
        pl.lit(None).alias("TrackingOption2"),
        pl.lit(None).alias("Currency"),
        pl.lit(None).alias("BrandingTheme"),
    )

    with pl.Config(tbl_cols=-1):
        print(result_df)

    result_df.write_csv("final.csv")


if __name__ == "__main__":
    main()
