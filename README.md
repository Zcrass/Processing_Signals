# Processing parquet data using Dask

A series of python programs that process data stored as parquet

## gap_detector_app
This program reads a parquet database with a 'Time' column and identify time gaps of a certain minimum length. The program reads the 'Time' column from the input database, identify gaps in the time sequence that are of a minimum defined length and save the data as a JSON table deposited in another path.

The program automatically identify if data exist in the output path and identify the last date stored in it. Then start looking for data posterior to the last date identify in the historical data.

### Input
The input is a parquet database in the format:

| Time                     | var1  | var2  | var3  | var4   |
|--------------------------| ----- | ----- | ----- | ------ |
| 2022-01-01 00:00:00.0000 |  9.5  |  7.7  |  8.2  |   7.7  |
| 2022-01-01 00:00:00.1000 |  9.4  |  7.8  |  8.1  |   7.1  |
| 2022-01-01 00:00:00.2000 |  9.3  |  7.7  |  8.1  |   7.8  |

### Output
The output is a JSON file in the format:

```json
[
    {
        "file":"./DATA/date=20221001/file_01.parquet",
        "init_gap":"2022-10-01 00:28:50",
        "end_gap":"2022-10-01 00:28:54"
    },
    {
        "file":"./DATA/date=20221001/file_02.parquet",
        "init_gap":"2022-10-01 00:32:54",
        "end_gap":"2022-10-01 00:47:39"
    },
    {
        "file":"./DATA/date=20221001/file_03.parquet",
        "init_gap":"2022-10-01 00:50:55",
        "end_gap":"2022-10-01 00:50:57"
    }
]
```

### Configuration
Cofiguration is done using a JSON file of format:

```json
{
    "data_path": "./DATA/",
    "output_data": "./GAPS/",
    "minimal_time_gap":"0.5 second",
    "split_data_stored_by": "day",
    "start_date":"20000101",
    "max_reads_attemps":5,
    "retry_sleep_time":10
}
```

## round2sec_app
This function reduce the amount of data stored in  parquet database by grouping the values by second and computing the mean. The program reads the 'Time' column and the defined signal columns from the database, round the time and group the data by second computing the mean value for each second. Resulting data is stored in a parquet table into the ouput path spliting the files by date.

The program automatically identify if data exist in the output path and identify the last date stored in it. Then start looking for data posterior to the last date identify in the historical data.

### Input
This program take as input a series of variable names and parquet database in the format:

| Time                     | var1  | var2  | var3  | var4   |
|--------------------------| ----- | ----- | ----- | ------ |
| 2022-01-01 00:00:00.0000 |  9.5  |  7.7  |  8.2  |   7.7  |
| 2022-01-01 00:00:00.1000 |  9.4  |  7.8  |  8.1  |   7.1  |
| 2022-01-01 00:00:00.2000 |  9.3  |  7.7  |  8.1  |   7.8  |

### Output
The output is a parquet database in the format:

| Time                | Value  | Date     | variable  |
|---------------------|--------|----------| --------- |
| 2022-01-01 00:00:01 | 9.5    | 20220101 |  var1     |
| 2022-01-01 00:00:02 | 9.4    | 20220101 |  var1     |
| 2022-01-01 00:00:03 | 7.7    | 20220101 |  var2     |
| 2022-01-01 00:00:01 | 7.7    | 20220101 |  var2     |
| 2022-01-01 00:00:02 | 8.1    | 20220101 |  var3     |
| 2022-01-01 00:00:03 | 8.2    | 20220101 |  var3     |

## Configuration
Cofiguration is done using a JSON file of format:
```json
{
    "input_data_path": "DATA/",
    "start_reading_date": "20221001",
    "time_column":"Time",
    "signals_to_include": ["var0",
                        "var1", 
                        "var2", 
                        "var3", 
                        "var4", 
                        "var5", 
                        "var6", 
                        "var7", 
                        "var8", 
                        "var9"],
    "output_data_path": "Out_Data/",
    "max_reads_attemps":5,
    "retry_sleep_time":10
}
```

## Content
This program includes the following files:
- gap_detector_app/main.py: correspond to the gap_detector_app program. Requires an argument -i (input) which indicates the path to the json config file
- gap_detector_app/utils.py: classes and functions used by the gap_detector_app.
- round2sec_app/main.py: correspond to the round2sec program. Requires an argument -i (input) which indicates the path to the json config file
- round2sec_app/utils.py: classes and functions used by the round2sec_app.
- configure_gap_detector.json: configuration file for the gap_detector_app
- configure_round2sec.json: configuration file for the round2sec_app
- requeriments.txt: list of dependencies required
- readme.md: this file
