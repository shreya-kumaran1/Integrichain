import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def model(dbt, session):
    """
    Final entity matching model.

    Outputs:
      NEW_ENTITY_ID,
      NEW_FULL_DETAILS,
      MATCHED_ID,
      MATCHED_FULL_DETAILS,
      MATCH_SCORE
    """

    dbt.config(
        materialized="table",
        alias="ENTITY_MATCH_RESULTS",
        packages=["pandas", "scikit-learn"]
    )

    # 1) Load master ("good") data: ID, FULL_DETAILS
    df_master = dbt.ref("master_data").to_pandas()

    # 2) Load new ("bad") data: ID, FULL_DETAILS
    df_new = dbt.ref("candidate_data").to_pandas()

    # 3) Fit TF-IDF on master FULL_DETAILS using 3-char n-grams
    vectorizer = TfidfVectorizer(
        analyzer="char",
        ngram_range=(3, 3),  # 3-character n-grams (same idea as blog)
        lowercase=False,
        min_df=1
    )

    tf_master = vectorizer.fit_transform(df_master["FULL_DETAILS"])
    tf_new = vectorizer.transform(df_new["FULL_DETAILS"])

    # 4) Cosine similarity: each new row vs all master rows
    sims = cosine_similarity(tf_new, tf_master)  # shape: (N_new, N_master)

    # 5) Best match index + score for each new row
    best_idx = sims.argmax(axis=1)        # index into df_master (0-based)
    best_scores = sims.max(axis=1)        # numpy array

    # 6) Get matched master rows by position
    df_master_reset = df_master.reset_index(drop=True)
    matched = df_master_reset.iloc[best_idx].copy()
    matched = matched.rename(columns={
        "ID": "MATCHED_ID",
        "FULL_DETAILS": "MATCHED_FULL_DETAILS"
    })

    # 7) Build the final result DataFrame directly (no concat on index)
    result = pd.DataFrame({
        "NEW_ENTITY_ID": df_new["ID"].reset_index(drop=True),
        "NEW_FULL_DETAILS": df_new["FULL_DETAILS"].reset_index(drop=True),
        "MATCHED_ID": matched["MATCHED_ID"].reset_index(drop=True),
        "MATCHED_FULL_DETAILS": matched["MATCHED_FULL_DETAILS"].reset_index(drop=True),
        "MATCH_SCORE": best_scores
    })

    return result