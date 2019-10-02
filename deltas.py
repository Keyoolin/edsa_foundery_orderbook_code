def merge_data(data_1, data_2, on, ascending = True):
    
    """
    Function to do a simple merge of the two dataframes inputted.
    
    Parameters
    ----------
        data_1 : Dataframe, required
        First Dataframe to merge on
        
        data_2 : Dataframe, required
        Second dataframe to merge on
        
        on : str, required
        The name of the column to merge on. 
        Column need to be present in both dataframes
        
        ascending : boolean, optional, default True
        Ascending or descending order of the dataframe.
        
    Returns
    ----------
        df : dataframe
        An outer merged dataframe with null values being filled with 0 values
        
    """
    
    
    df = data_1.merge(data_2, on = on, how = 'outer')
    df = df.sort_values(by = on, ascending = ascending)
    df = df.fillna(0)
    return df

def time_delta(data_1, data_2):
    return data_2.iloc[1].timestamp - data_1.iloc[1].timestamp

def clean_data(data, which = 'asks', volume_column = 'volume'):

    """
    
    clean_data removes the unneeded volume columns after the merge. 
    The volume columns provides a means of calculating the deltas,
    however post calculation, it's not needed.
    
    Parameters
    ----------
        data : Pandas dataframe, required
        Dataframe to clean
        
        which : str, "bids" | "asks", optional, default = "asks"
        Accepts "bids" or "asks" as which column to clean.
        
        volume_column : str, optional, default = "volume"
        Naming convention of the volume column. 
        "bids_volume" vs "bids_quantity", etc.
        
    Returns
    ----------
        df : dataframe
        An cleaned dataframe with t0 and t1 volume columns deleted.
        
    """

    return data.drop(['{}_{}_t0'.format(which, volume_column), '{}_{}_t1'.format(which, volume_column)], axis = 1)
    
def column_names(which, columns):
        price_column = "{}_{}".format(which, columns[0]) 
        volume_column = "{}_{}".format(which, columns[1])
        
        return price_column, volume_column
    
def calculate_deltas(snapshot_1, snapshot_2, which, name = 'time_format', columns = ['price', 'volume']):

    """
    calculate_deltas calculates the change in volume between two snapshots
    
    Parameters
    ----------
        snapshot_1 : dataframe, required
        Snapshot orderbook data at time T0
        
        snapshot_2 : Dataframe, required
        Snapshot orderbook data at time T0
        
        which : str, "bids" | "asks" , required
        Which deltas to calculate, bids or asks
        
        name : str, "none" | "time_format", optional, default = "time_format"
        Selects the naming format of the returned delta column. 
        Time_format calculates the time difference and appends it to the column name.
        None names it a delta column.
        
        columns : list, optional, default = ['price', 'volume']
        Naming convention of the price and volume column. 
        "bids_volume" vs "bids_quantity", etc.
        column names must be in the format "{1}_{2}", where 1 is the which parameter
        and 2 is the column name parameter.
        
        
    Returns
    ----------
        merged_bids : Dataframe 
        Dataframe of deltas between snapshot_1 and snapshot_2.
        The dataframe returned would either be of bids or of asks.
            
    """
            
    price_column, volume_column = column_names(which, columns)
    
    data_1 = snapshot_1[[price_column, volume_column]]
    data_2 = snapshot_2[[price_column, volume_column]]
    
    if which == "bids":
        ascending = False
    elif which == "asks":
        ascending = True
        
    merged_data = merge_data(data_1, data_2, on = price_column, ascending = ascending)
    merged_data.columns = [price_column, "{}_t0".format(volume_column) , "{}_t1".format(volume_column)]
    
    store = merged_data["{}_t1".format(volume_column)] - merged_data["{}_t0".format(volume_column)]
    
    if name == 'time_format':
        merged_data['delta_{}_{}'.format(which, time_delta(snapshot_1, snapshot_2))] = store
    elif name == 'none': 
        merged_data['delta_{}'.format(which)] = store    
    
    return merged_data

def calculate_multiple_deltas(data, first, count, which, name = 'time_format', columns = ['price', 'volume']):


    """
    Calculates the change in volume between multiple snapshots
    
    Parameters
    ----------
    
        data : list, required
        List of snapshots saved as dataframes
        
        first : int, required
        The initial start point in the list of snapshots for which to calculate deltas
        
        count : int, list or tuple, required
        How many snapshots in the future should the delta's be calculated for?
        if list is used, calculates multiple deltas. e.g. between t0 and t2, and t0 and t3
        
        which : str, "asks" | "bids", required
        Which deltas to calculate, bids or asks
        
        name : str, "none" | "time_format", optional, default = 'time_format'
        Selects the naming format of the returned delta column. 
        Time_format calculates the time difference and appends it to the column name.
        None names it a delta column.
        
        columns : list, optional, default = ['price', 'volume'] 
        Naming convention of the price and volume column.
        Column names must be in the format "{1}_{2}", where 1 is the which parameter
        and 2 is the column name parameter.
        
        
    Returns
    ----------
        deltas_df : Dataframe
        Dataframe of delta values between snapshots
        
    """
    
    if type(data) != list or type(first) != int:
        raise TypeError("Incorrect type format of input data")
           

    if type(count) == list or type(count) == tuple:
        if max(count) > (len(data) - first - 1):
            raise ValueError("Not enough data to calculate delta's. Shorten t1 request")
            
        counter = 0
        for i in count:

            deltas = calculate_deltas(data[first],
                                              data[first+i],
                                              which = which,
                                              name = name,
                                              columns = columns)
                
            if which == 'bids':
                ascending = False
            elif which == 'asks':
                ascending = True

            price_column, volume_column = column_names(which, columns)

            if counter == 0:
                deltas_df = deltas.drop('{}_t1'.format(volume_column), axis = 1)
            else:
                deltas_df = clean_data(deltas_df,
                                       clean_data(deltas, which),
                                       on = price_column,
                                       ascending = ascending)
                
            counter = counter + 1
        return deltas_df
        
    elif count > (len(data) - first - 1):
        raise ValueError("Not enough data to calculate delta's. Shorten t1 request")
    else:
        return calculate_deltas(data[first], data[first+count], which = which, name = name, columns = columns)