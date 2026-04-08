from datetime import datetime

def convert_stringdate_to_date(string):
    try:
        datetime_date = datetime.strptime(string, "%b %d %Y").date()
        date_iso = datetime_date.isoformat()
        return date_iso
    except ValueError:
        raise ValueError("Invalid date format. Please use 'Aug 17 2024' format.")
    
def get_current_season():
    now = datetime.now()
    if now.month >= 7:
        return f"{now.year}/{str(now.year + 1)[-2:]}"
    else:
        return f"{now.year - 1}/{str(now.year)[-2:]}"
    
def match_names_fuzzy(df, match_df, score_threshold=80, id_col_name='player_id'):
    from thefuzz import process

    names = df['name'].tolist()
    names_to_match = match_df['name'].tolist()

    if id_col_name not in df.columns:
        df[id_col_name] = None

    for i, name in enumerate(names):
        best_match = process.extractOne(name, names_to_match)
        if best_match and best_match[1] >= score_threshold:
            df[id_col_name].iloc[i] = match_df[match_df['name'] == best_match[0]][id_col_name].values[0]
    
    return df