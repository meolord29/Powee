import polars as pl
import csv
from datetime import datetime, timedelta

class Analyser: 
    def __init__(self, csv_path: str):
        self.schema = {"\ufeffAccount Number 編賬號碼": pl.Int64,
                       "Start date/time 開始日期/時間": pl.Utf8,  # Read as string
                       "End date/time 結束日期/時間": pl.Utf8,    # Read as string
                       "Total Consumption (Unit) 總用電量 (度數)": pl.Float32}
        self.df = None
        self.datetime_format = "%d/%m/%Y %H:%M"
        self.csv_path = csv_path

    def csv_clip_csv(self):
        with open(self.csv_path, 'r') as file:
            reader = csv.reader(file)

            for i, row in enumerate(list(reader)):
                if row == []:
                    cutOff = i - 1
                    break
        with open(self.csv_path, 'r') as file:
            csv_list = list(csv.DictReader(file))[:cutOff]

            df = pl.DataFrame(csv_list, schema=self.schema)
            df = df.rename({
                "\ufeffAccount Number 編賬號碼": "AccID", 
                "Total Consumption (Unit) 總用電量 (度數)": "UnitConsumption", 
                "Start date/time 開始日期/時間": "StartTime", 
                "End date/time 結束日期/時間": "EndTime"
                })
        self.df = df
        return 0

    def csv_parse_time(self):
        df_copy = self.df.clone()
        # Convert string columns to Datetime type using the specified format
        df_copy = df_copy.with_columns(
            pl.col("StartTime").str.to_datetime(format=self.datetime_format),
            pl.col("EndTime").str.to_datetime(format=self.datetime_format)
        )
        self.df = df_copy
        return 0

    def get_total_consumption(self) -> float:
        return self.df.select(["UnitConsumption"]).sum()
    
    def get_consumption_by_date(self, startTime: datetime, endTime: datetime) -> float:
        df_filtered = self.get_df_by_time_range(startTime, endTime)
        
        return df_filtered.select(["UnitConsumption"]).sum()

    def get_df_by_time_range(self, startTime: datetime, endTime: datetime) -> pl.DataFrame:
        df_filtered = self.df.filter(
            (pl.col("StartTime") >= startTime) & (pl.col("EndTime") <= endTime)
            )
        
        return df_filtered

    def get_total_time_by_date(self, startTime: datetime, endTime: datetime) -> timedelta:
        df_filtered = self.get_df_by_time_range(startTime, endTime)
        maxTime, minTime = df_filtered.select(["StartTime"]).max(), df_filtered.select(["EndTime"]).min()
        return maxTime - minTime

    def get_total_time_range(self) -> tuple[datetime, datetime]:
        return self.df.select(["StartTime"]).max(), self.df.select(["EndTime"]).min()

    def get_total_time(self) -> timedelta:
        maxTime, minTime = self.get_total_time_range()
        return maxTime - minTime, (maxTime, minTime)

if __name__ == "__main__":
    clp_analyzer = Analyser("./test_files/consumption_history.csv")
    clp_analyzer.csv_clip_csv()
    clp_analyzer.csv_parse_time()
    print(clp_analyzer.get_total_time_range())
    print(clp_analyzer.get_total_consumption())
    print(clp_analyzer.get_total_time())
    print(clp_analyzer.get_consumption_by_date(datetime(2025, 5, 24, 0, 0), datetime(2025, 6, 1, 0, 0)))
    print(clp_analyzer.get_total_time_by_date(datetime(2025, 5, 24, 0, 0), datetime(2025, 6, 1, 0, 0)))