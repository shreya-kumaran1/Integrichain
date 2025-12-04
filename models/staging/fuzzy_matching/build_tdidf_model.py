import io
import pickle
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

# These are for our own string building only – NOT used in dbt.config
DB_NAME = "SHREYA_SANDBOX"
SCHEMA_NAME = "CUSTOMERS"
STAGE_NAME = "MODEL_DATA"
# Fully-qualified stage name: "SHREYA_SANDBOX"."CUSTOMERS".MODEL_DATA
STAGE_FQN = f'"{DB_NAME}"."{SCHEMA_NAME}".{STAGE_NAME}'

def save_file(session, model, stage_path: str):
    """
    Equivalent to the blog's save_file():
    - serialize object with pickle
    - upload to stage (e.g. @\"SHREYA_SANDBOX\".\"CUSTOMERS\".MODEL_DATA/tf_idf.pickle)
    """
    input_stream = io.BytesIO()
    pickle.dump(model, input_stream)
    input_stream.seek(0)

    session._conn._cursor.upload_stream(input_stream, stage_path)

def model(dbt, session):
    """
    dbt entrypoint; logically the same as build_tdidf_model(session) in the article.
    """
    # IMPORTANT: only literals in dbt.config
    dbt.config(
        materialized="table",
        alias="INDEX",  # table name INDEX in your target db/schema
        packages=["pandas", "scikit-learn"] 
        # no database=, schema=, tags=, packages= here to avoid parser issues
    )

    # 1) Read MASTER_DATA (ID, FULL_DETAILS)
    #    This MUST be a literal string for dbt.ref
    master_df = dbt.ref("master_data").to_pandas()

    # 2) TF-IDF on FULL_DETAILS using character 3-grams (n-grams like the guide)
    indexes = master_df["FULL_DETAILS"]

    vectorizer = TfidfVectorizer(
        analyzer="char",
        ngram_range=(3, 3),  # character trigrams
        lowercase=False,
        min_df=1
    )

    tf_idf_matrix = vectorizer.fit_transform(indexes)

    # 3) Build INDEX table: ID, FULL_DETAILS, IDX (1-based index)
    df_index = pd.DataFrame({
        "ID": master_df["ID"],
        "FULL_DETAILS": master_df["FULL_DETAILS"],
        "IDX": range(1, len(master_df) + 1)
    })

    # 4) Ensure fully qualified stage exists
    session.sql(f"CREATE STAGE IF NOT EXISTS {STAGE_FQN}").collect()

    # 5) Save TF-IDF matrix and vectorizer to @MODEL_DATA (fully qualified)
    save_file(session, tf_idf_matrix, f'@{STAGE_FQN}/tf_idf.pickle')
    save_file(session, vectorizer,     f'@{STAGE_FQN}/vectorizer.pickle')

    # 6) Returning df_index -> dbt materializes SHREYA_SANDBOX.CUSTOMERS.INDEX
    return df_index