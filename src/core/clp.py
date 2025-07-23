import polars as pl
import csv

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

    def get_total_time_range(self):
        return self.df.select(["StartTime"]).min(), self.df.select(["EndTime"]).max()

    def get_total_consumption(self):
        return self.df.select(["UnitConsumption"]).sum()

    def get_total_time(self):
        return self.df.select(["EndTime"]).max() - self.df.select(["StartTime"]).min()



if __name__ == "__main__":
    clpAnalyzer = Analyser("./test_files/consumption_history.csv")
    clpAnalyzer.csv_clip_csv()
    clpAnalyzer.csv_parse_time()
    print(clpAnalyzer.get_total_time_range())
    print(clpAnalyzer.get_total_consumption())
    print(clpAnalyzer.get_total_time())