# Processing parquet data using Dask

A series of python programs that process data stored as parquet.

## gaps.py
This program reads a parquet database and identify gaps in a 'Time' column.

The input is a parquet database in the format:
| Time                     | var1  | var2  | var3  | var4   |
|--------------------------| ----- | ----- | ----- | ------ |
| 2022-01-01 00:00:00.0000 |  9.5  |  7.7  |  8.2  |   7.7  |
| 2022-01-01 00:00:00.1000 |  9.4  |  7.8  |  8.1  |   7.1  |
| 2022-01-01 00:00:00.2000 |  9.3  |  7.7  |  8.1  |   7.8  |

The output is a JSON file in the format:

```
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
Cofiguration is done using a JSON file of format:

```
{
    "input": "./DATA/",
    "out_path": "./gaps",
    "time_gap":"1 second",
    "store_by": "day",
    "max_attemps":5,
    "sleep_time":10
}
```

## round2sec.py
Their function is to reduce the amount of data grouping the values by second and computing the mean.

This program take as input a series of variable names and parquet database in the format:
| Time                     | var1  | var2  | var3  | var4   |
|--------------------------| ----- | ----- | ----- | ------ |
| 2022-01-01 00:00:00.0000 |  9.5  |  7.7  |  8.2  |   7.7  |
| 2022-01-01 00:00:00.1000 |  9.4  |  7.8  |  8.1  |   7.1  |
| 2022-01-01 00:00:00.2000 |  9.3  |  7.7  |  8.1  |   7.8  |

The output is a parquet database in the format:

| Time                | Value  | Date     | variable  |
|---------------------|--------|----------| --------- |
| 2022-01-01 00:00:01 | 9.5    | 20220101 |  var1     |
| 2022-01-01 00:00:02 | 9.4    | 20220101 |  var1     |
| 2022-01-01 00:00:03 | 7.7    | 20220101 |  var2     |
| 2022-01-01 00:00:01 | 7.7    | 20220101 |  var2     |
| 2022-01-01 00:00:02 | 8.1    | 20220101 |  var3     |
| 2022-01-01 00:00:03 | 8.2    | 20220101 |  var3     |


Cofiguration is done using a JSON file of format:
```
{
    "input": "DATA/",
    "start_date": "20221001",
    "var_cols": [
        "var_01",
        "var_02",
        "var_03",
        "var_04",
        "var_05",
        "var_06",
        "var_07",
        "var_08",
        "var_09",
        "var_10"
        ],
    "out_path": "Out_Data/",
    "max_attemps":5,
    "sleep_time":10
}
```

