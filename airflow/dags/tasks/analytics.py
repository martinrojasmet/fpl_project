import sys
sys.path.append('/opt/airflow/dags')

from airflow.decorators import task
import pandas as pd
import numpy as np
from utils.storage.analytics import get_point_prediction_training_input, get_next_gameweeks, add_point_prediction
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import HistGradientBoostingRegressor

def build_imputation_lookups(X_train: pd.DataFrame, missing_values_columns: list) -> dict:
    lookups = {}

    last_5_cols = [c for c in missing_values_columns if c.endswith('last_5')]
    overall_cols = [c for c in missing_values_columns if c.endswith('overall')]
    vs_opp_cols = [c for c in missing_values_columns if c.endswith('vs_opponent')]
    prior_cols = [c for c in missing_values_columns if c.endswith('prior')]

    # Global means across all rows in X_train
    global_cols = last_5_cols + overall_cols
    if global_cols:
        lookups['global'] = X_train[global_cols].mean()

    # Grouped means by opponent_team_id in X_train
    opponent_cols = vs_opp_cols + prior_cols
    if opponent_cols:
        lookups['opponent'] = X_train.groupby('opponent_team_id')[opponent_cols].mean()
        lookups['extra_opponent'] = X_train[opponent_cols].mean()

    return lookups

def fill_rolling_missing_values(df: pd.DataFrame, missing_values_columns: list, lookups: dict) -> pd.DataFrame:
    df_imputed = df.copy()

    last_5_cols = [c for c in missing_values_columns if c.endswith('last_5')]
    overall_cols = [c for c in missing_values_columns if c.endswith('overall')]
    vs_opp_cols = [c for c in missing_values_columns if c.endswith('vs_opponent')]
    prior_cols = [c for c in missing_values_columns if c.endswith('prior')]

    global_cols = last_5_cols + overall_cols
    opp_cols = vs_opp_cols + prior_cols

    # Global columns fillna
    if global_cols and 'global' in lookups:
        df_imputed[global_cols] = df_imputed[global_cols].fillna(lookups['global'])

    # Opponent specific columns fillna
    if opp_cols and 'opponent' in lookups:
        # 1. Primary Imputation: Match opponent_team_id to X_train opponent averages
        opp_means = df_imputed[['opponent_team_id']].merge(
            lookups['opponent'],
            left_on='opponent_team_id',
            right_index=True,
            how='left'
        )
        opp_means.index = df_imputed.index
        df_imputed[opp_cols] = df_imputed[opp_cols].fillna(opp_means[opp_cols])

        df_imputed[opp_cols] = df_imputed[opp_cols].fillna(lookups['extra_opponent'])

    return df_imputed

@task
def point_prediction() -> pd.DataFrame:
    df = get_point_prediction_training_input()

    input_prediction_df = get_next_gameweeks()
    input_prediction_df.rename(columns={'fpl_game_id': 'game_id'}, inplace=True)

    # Keep numeric and boolean columns, drop non-numeric columns except boolean
    non_numeric_columns = df.select_dtypes(exclude=[np.number]).columns
    bool_columns = df.select_dtypes(include=[bool]).columns
    string_cols = df.select_dtypes(include=[object]).columns
    extra_columns_to_exclude = ['game_id']
    columns_to_exclude = list((set(non_numeric_columns) | set(extra_columns_to_exclude)) - set(bool_columns))

    df = df.sort_values(by='datetime', ascending=True).copy()

    # Filter DataFrame and convert boolean columns to int
    numeric_df = df[df.columns.difference(columns_to_exclude)]
    numeric_df[bool_columns] = numeric_df[bool_columns].astype(int)
    numeric_non_null_df = numeric_df.dropna()

    numeric_input_df = input_prediction_df[input_prediction_df.columns.difference(columns_to_exclude)]
    numeric_input_df[bool_columns] = numeric_input_df[bool_columns].astype(int)
    X_pred = numeric_input_df.dropna()

    # Data Input/Output split
    data_ids = df[string_cols]

    # Output
    target_value = 'points'
    y = df[target_value]

    # Input
    columns_to_exclude_input = columns_to_exclude + [target_value]
    X = df.drop(columns=columns_to_exclude_input)

    # Data Train/Val/Test split
    len_data = len(X)
    train_percent = 0.65
    val_percent = 0.80

    train_end = int(train_percent * len_data)
    val_end = int(val_percent * len_data)

    # Input splits
    X_train_split = X.iloc[:train_end]
    X_val_split   = X.iloc[train_end:val_end]
    X_test_split  = X.iloc[val_end:]

    # Output splits
    y_train = y.iloc[:train_end]
    y_val   = y.iloc[train_end:val_end]
    y_test  = y.iloc[val_end:]

    # Attach string identifiers (like opponent_team_id) to features for mapping
    X_train_ids = data_ids.iloc[:train_end]
    X_train_identified = pd.concat([X_train_ids, X_train_split], axis=1)

    X_val_ids = data_ids.iloc[train_end:val_end]
    X_val_identified = pd.concat([X_val_ids, X_val_split], axis=1)

    X_test_ids = data_ids.iloc[val_end:]
    X_test_identified = pd.concat([X_test_ids, X_test_split], axis=1)

    # Fillig up missing values with mean from training data
    missing_values_columns = X_train_split.columns[X_train_split.isnull().any()].tolist()
    rolling_imputations = build_imputation_lookups(X_train_identified, missing_values_columns)

    # Input
    X_train_filled = fill_rolling_missing_values(X_train_identified, missing_values_columns, rolling_imputations)
    X_val_filled   = fill_rolling_missing_values(X_val_identified, missing_values_columns, rolling_imputations)
    X_test_filled  = fill_rolling_missing_values(X_test_identified, missing_values_columns, rolling_imputations)

    # Output
    X_train = X_train_filled.drop(columns=string_cols)
    X_val = X_val_filled.drop(columns=string_cols)
    X_test = X_test_filled.drop(columns=string_cols)

    X_val = X_val[X_train.columns]
    X_test = X_test[X_train.columns]
    X_pred = X_pred[X_train.columns]

    scaler = StandardScaler()
    X_train_scaled = pd.DataFrame(
        scaler.fit_transform(X_train),
        columns=X_train.columns,
        index=X_train.index
    )
    X_val_scaled = pd.DataFrame(
        scaler.transform(X_val),
        columns=X_val.columns,
        index=X_val.index
    )
    X_test_scaled = pd.DataFrame(
        scaler.transform(X_test),
        columns=X_test.columns,
        index=X_test.index
    )

    X_pred_scaled = pd.DataFrame(
        scaler.transform(X_pred),
        columns=X_pred.columns,
        index=X_pred.index
    )

    # Scaled DataFrames to NumPy arrays for model input
    X_train_np = np.asarray(X_train_scaled)
    X_val_np = np.asarray(X_val_scaled)
    X_test_np = np.asarray(X_test_scaled)

    y_train_np = np.asarray(y_train).ravel().astype(float)
    y_val_np = np.asarray(y_val).ravel().astype(float)
    y_test_np = np.asarray(y_test).ravel().astype(float)

    # Regression model for point prediction
    point_model = HistGradientBoostingRegressor(
        loss='squared_error',
        learning_rate=0.08,
        max_iter=250,
        max_leaf_nodes=31,
        l2_regularization=1.0,
        random_state=42,
    )
    point_model.fit(X_train_np, y_train_np)

    Y_pred = point_model.predict(X_pred_scaled)
    X_pred_ids = input_prediction_df[columns_to_exclude].copy()

    Y_pred_series = pd.Series(Y_pred, index=X_pred_ids.index, name='predicted_points')

    prediction_df = pd.concat([X_pred_ids, Y_pred_series], axis=1)
    prediction_df.rename(columns={'game_id': 'fpl_game_id'}, inplace=True)

    add_point_prediction(prediction_df)
