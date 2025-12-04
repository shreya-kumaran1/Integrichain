import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

def model(dbt, session):
    dbt.config(
        materialized="table",
        alias="INDEX",  # matches article naming
        packages=["pandas", "scikit-learn"]
    )

    # MASTER_DATA: ID, FULL_DETAILS
    data = dbt.ref("master_data").to_pandas()

    # Fit TF-IDF (char 3-grams) on FULL_DETAILS – same as blog’s n-gram approach
    vectorizer = TfidfVectorizer(
        analyzer="char",
        ngram_range=(3, 3),
        lowercase=False,
        min_df=1
    )
    _ = vectorizer.fit_transform(data["FULL_DETAILS"])

    # Build index table with 1-based IDX like the article
    df_index = pd.DataFrame({
        "ID": data["ID"],
        "FULL_DETAILS": data["FULL_DETAILS"],
        "IDX": range(1, len(data) + 1)
    })

    return df_index