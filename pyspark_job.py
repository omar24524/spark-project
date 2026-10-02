from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_data(df: DataFrame) -> DataFrame:
    """
    Cleans the input DataFrame by:
    1. Removing rows where amount <= 0
    2. Removing rows where name is NULL
    3. Adding an 'amount_with_tax' column (amount * 1.20)
    """
    cleaned_df = (
        df.filter((F.col("amount") > 0) & (F.col("name").isNotNull()))
        .withColumn("amount_with_tax", F.col("amount") * 1.20)
    )
    return cleaned_df