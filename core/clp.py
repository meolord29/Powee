import csv
import pandas as pd

datetime_format = "%d/%m/%Y %H:%M"

def __csv_extract(filepath):
    with open(filepath, 'r') as file:
        reader = csv.reader(file)

        for i, row in enumerate(list(reader)):
            if row == []:
                cutOff = i - 1
                break
    
    with open(filepath, 'r') as file:
        csv_list = list(csv.DictReader(file))[:cutOff]

        df = pd.DataFrame(csv_list)
        df = df.rename(columns={
            "\ufeffAccount Number 編賬號碼": "AccID", 
            "Total Consumption (Unit) 總用電量 (度數)": "UnitConsumption", 
            "Start date/time 開始日期/時間": "StartTime", 
            "End date/time 結束日期/時間": "EndTime"
            })

        return df

def __csv_parse_time(df) -> pd.DataFrame:
    df["StartTime"] = pd.to_datetime(df["StartTime"], format=datetime_format)
    df["EndTime"] = pd.to_datetime(df["EndTime"], format=datetime_format)
    df["UnitConsumption"] = pd.to_numeric(df["UnitConsumption"])
    return df

def parse(filepath) -> pd.DataFrame:
    df = __csv_extract(filepath)
    df = __csv_parse_time(df)
    return df