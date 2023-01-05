def subranges(cols, num_sub_ranges, batch_size):
    start_list = []
    end_list = []
    for i in range(0, num_sub_ranges):
        start = (i*batch_size)+1
        end = start + batch_size
        if end > len(cols):
            end = len(cols)
        start_list.append(start)
        end_list.append(end)
    return start_list, end_list

def signal_df(df, signal):
    ''' function to convert df format to output table format '''
    signal_df = df.groupby(df["Time"],)[signal].mean().reset_index()
    signal_df["Date"] = df["Time"].dt.floor("d",).dt.strftime("%Y%m%d")
    signal_df["Signal"] = signal
    names = ["Time", "Value", "Date", "Signal"]
    return signal_df.rename(columns=dict(zip(signal_df.columns, names)))


# def name_function(part_ind):
#     Date = table["Date"].iat[0]
#     name = Date + "_" + format(part_ind, '06d') + ".parquet"
#     name =  format(part_ind, '06d') + ".parquet"
#     return name