from datetime import datetime
import pandas as pd
import os

def convert_stringdate_to_date(string):
    try:
        datetime_date = datetime.strptime(string, "%b %d %Y").date()
        date_iso = datetime_date.isoformat()
        return date_iso
    except ValueError:
        raise ValueError("Invalid date format. Please use 'Aug 17 2024' format.")
    
def get_current_season():
    now = datetime.now()
    if now.month >= 7 and now.day > 25:
        return f"{now.year}/{str(now.year + 1)[-2:]}"
    else:
        return f"{now.year - 1}/{str(now.year)[-2:]}"

def normalize_name(value):
    import html
    import unicodedata
    
    if not isinstance(value, str):
        return value
    value = html.unescape(value)
    value = value.replace("’", "'").replace("`", "'").replace("´", "'")
    value = unicodedata.normalize("NFKC", value)
    return value

def remove_accents(text):
    import unicodedata
    if not isinstance(text, str):
        return text
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


# Matching helper functions
def intermediate_mapping_matching(
    raw_data_df,
    mapping_df,
    raw_key="name",
    mapping_key=None,
):
    mapping_key = mapping_key or raw_key

    if raw_data_df.empty:
        original_columns = list(raw_data_df.columns)
        result_columns = original_columns + list(set(mapping_df.columns) - set(original_columns))
        return pd.DataFrame(columns=result_columns), raw_data_df.copy()

    matched_df = raw_data_df.merge(
        mapping_df,
        left_on=raw_key,
        right_on=mapping_key,
        how="inner",
        suffixes=("", "_mapping"),
    )

    unmatched_df = raw_data_df.loc[
        ~raw_data_df[raw_key].isin(matched_df[raw_key])
    ].copy()

    print(f"Intermediate mapping matched {len(matched_df)}, {len(unmatched_df)} unmatched.")

    return matched_df, unmatched_df

def fuzzy_string_matching(
    raw_data_df,
    db_data_df,
    raw_name_col="name",
    db_name_col="name",
    result_field="player_id",
    threshold=86,
):
    from thefuzz import process, fuzz

    result_columns = list(raw_data_df.columns) + [result_field]

    if raw_data_df.empty:
        return pd.DataFrame(columns=result_columns), raw_data_df.copy()

    matched_rows = []
    unmatched_rows = []

    db_choices = db_data_df[db_name_col].dropna().tolist()

    db_lookup = (
        db_data_df.dropna(subset=[db_name_col, result_field])
        .drop_duplicates(subset=[db_name_col])
        .set_index(db_name_col)[result_field]
        .to_dict()
    )

    for _, row in raw_data_df.iterrows():
        raw_name = row[raw_name_col]
        best = process.extractOne(raw_name, db_choices, scorer=fuzz.token_sort_ratio)

        if best is None or best[1] < threshold:
            unmatched_rows.append(row.to_dict())
            continue

        matched_row = row.to_dict()
        matched_row[result_field] = db_lookup.get(best[0])
        matched_rows.append(matched_row)

    print(f"Fuzzy matched {len(matched_rows)}, {len(unmatched_rows)} unmatched.")

    matched_df = pd.DataFrame(matched_rows)
    unmatched_df = pd.DataFrame(unmatched_rows)

    if matched_df.empty:
        empty_match_df = pd.DataFrame(columns=result_columns)
        return empty_match_df, raw_data_df.copy()

    return matched_df, unmatched_df

def ai_matching(
    raw_data_df,
    db_data_df,
    prompt_string,
    response_json_schema,
    ai_model="gemini-2.5-flash",
    raw_match_col="name",
    result_field="player_id"
):
    from google import genai
    from google.genai import types
    import json

    if raw_data_df.empty:
        original_columns = list(raw_data_df.columns)
        result_columns = original_columns + list(set(db_data_df.columns) - set(original_columns))
        return pd.DataFrame(columns=result_columns), raw_data_df.copy()

    GOOGLE_AI_API_KEY = os.getenv("GOOGLE_AI_API_KEY")

    if not GOOGLE_AI_API_KEY:
        raise ValueError("GOOGLE_AI_API_KEY is not set in the environment.")

    if raw_data_df.empty:
        return raw_data_df.copy(), raw_data_df.copy()

    client = genai.Client(api_key=GOOGLE_AI_API_KEY)

    raw_ascii_df = raw_data_df.copy()
    raw_ascii_df[raw_match_col] = raw_ascii_df[raw_match_col].apply(remove_accents)

    raw_ascii_map = dict(
        zip(raw_ascii_df[raw_match_col], raw_data_df[raw_match_col])
    )

    input_data = {
        "unmatched_raw_data": raw_ascii_df.to_dict(orient="records"),
        "data_in_db": db_data_df.to_dict(orient="records"),
    }

    prompt = prompt_string + json.dumps(input_data)

    response = client.models.generate_content(
        model=ai_model,
        contents=prompt,
        config={
            "response_mime_type": "application/json",
            "response_schema": list[response_json_schema],
            "temperature": 0.0,
            # "max_output_tokens": 8192,
        },
    )

    with open("ai_response_now.txt", "w") as f:
        f.write(response.text or "")

    matched_data = json.loads(response.text or "[]")
    matched_df = pd.DataFrame(matched_data)

    if matched_df.empty:
        matched_df = pd.DataFrame(columns=list(raw_data_df.columns) + [result_field])
        return matched_df, raw_data_df.copy()

    matched_df[raw_match_col] = matched_df[raw_match_col].map(raw_ascii_map)

    unmatched_df = raw_data_df.loc[
        ~raw_data_df[raw_match_col].isin(matched_df[raw_match_col])
    ].copy()

    print(f"AI matched {len(matched_df)}, {len(unmatched_df)} unmatched.")

    return matched_df, unmatched_df


# FPL API helper functions
def fetch_player_fpl_api(player_id):
    import requests

    r = requests.get(
        f"https://fantasy.premierleague.com/api/element-summary/{player_id}/",
        timeout=30
    )
    r.raise_for_status()
    return player_id, r.json()