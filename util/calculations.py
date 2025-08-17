from datetime import datetime
import pandas as pd
import numpy as np


def filter_unit_consumption_by_time(df, startTime=9, endTime=17) -> pd.DataFrame:
    time_mask = (df['StartTime'].dt.hour >= startTime) & (df['StartTime'].dt.hour <= endTime)
    day_mask = df['StartTime'].dt.dayofweek < 5
    working_hours_df = df[time_mask & day_mask]
    return working_hours_df


def calc_server_consumption(df: pd.DataFrame, consumptionField = "UnitConsumption"):
    consumptionSeries = filter_unit_consumption_by_time(df)[consumptionField]
    C_s = consumptionSeries.mean() * len(df)
    return C_s

def calc_total_consumption(df: pd.DataFrame, consumptionField="UnitConsumption"):
    C_t = df[consumptionField].sum()
    return C_t


def calc_server_bill(billTotal: float, df: pd.DataFrame, consumptionField="UnitConsumption"):
    C_s = calc_server_consumption(df=df, consumptionField=consumptionField)
    C_t = calc_total_consumption(df=df, consumptionField=consumptionField)
    B_t = billTotal

    B_s = B_t * (C_s/C_t)
    return B_s