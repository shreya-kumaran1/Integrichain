import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def model(dbt, session):
    """
    Final entity matching model.

    Algorithm:
      - Use INDEX (ID, FULL_DETAILS) as master/golden data.
      - Use CANDIDATE_DATA as "new" entities.
      - Fit TF-IDF with char 3-grams on master FULL_DETAILS.
      - Transform new FULL_DETAILS with same vectorizer.
      - Compute cosine similarity to find best match.
      - Return: NEW_ENTITY_ID, NEW_FULL_DETAILS, MATCHED_ID, MATCHED_FULL_DETAILS, MATCH_SCORE
    """

    dbt.config(
        materialized="table",
        alias="ENTITY_MATCH_RESULTS",
        packages=["pandas", "scikit-learn"]
    )

    # 1) Master/golden data – via INDEX from build_tdidf_model
    df_index = dbt.ref("build_tdidf_model").to_pandas()  # ID, FULL_DETAILS, IDX

    # 2) New entities to match
    df_new = dbt.ref("candidate_data").to_pandas()       # ID, FULL_DETAILS

    # 3) TF-IDF with char 3-grams on master FULL_DETAILS
    vectorizer = TfidfVectorizer(
        analyzer="char",
        ngram_range=(3, 3),
        lowercase=False,
        min_df=1
    )

    tf_master = vectorizer.fit_transform(df_index["FULL_DETAILS"])
    tf_new = vectorizer.transform(df_new["FULL_DETAILS"])

    # 4) Cosine similarity: each new row vs all master rows
    sims = cosine_similarity(tf_new, tf_master)  # shape: (N_new, N_master)

    # 5) Best match index + score per new row
    best_idx = sims.argmax(axis=1)       # 0-based position into df_index
    best_scores = sims.max(axis=1)

    # 6) Get matched master rows by position
    df_index_reset = df_index.reset_index(drop=True)
    matched = df_index_reset.iloc[best_idx].copy()
    matched = matched.rename(columns={
        "ID": "MATCHED_ID",
        "FULL_DETAILS": "MATCHED_FULL_DETAILS"
    })

    # 7) Build final result table
    result = pd.DataFrame({
        "NEW_ENTITY_ID": df_new["ID"].reset_index(drop=True),
        "NEW_FULL_DETAILS": df_new["FULL_DETAILS"].reset_index(drop=True),
        "MATCHED_ID": matched["MATCHED_ID"].reset_index(drop=True),
        "MATCHED_FULL_DETAILS": matched["MATCHED_FULL_DETAILS"].reset_index(drop=True),
        "MATCH_SCORE": best_scores
    })

    return result