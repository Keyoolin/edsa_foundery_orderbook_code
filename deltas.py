def merge_data(data_1, data_2, on, ascending = True):
    
     """
    Parameters:
        data_1 (dataframe): First dataframe to merge on
        data_2 (dataframe): Second dataframe to merge on
        on (str): The name of the column to merge on. Column need to be present in both dataframes
        ascending (bool): True or False. Ascending or descending order of the dataframe.
        
    Returns:
        df (dataframe): An outer merged dataframe with null values being filled with 0 values
        
    """
    
    
    df = data_1.merge(data_2, on = on, how = 'outer')
    df = df.sort_values(by = on, ascending = ascending)
    df = df.fillna(0)
    return df

def time_delta(data_1, data_2):
    return data_2.iloc[1].timestamp - data_1.iloc[1].timestamp

def clean_data(data, which, volume_column = 'volume'):
    if which == 'asks':
        return data.drop(['asks_{}_t0'.format(volume_column), 'asks_{}_t1'.format(volume_column)], axis = 1)
    elif which == 'bids':
        return data.drop(['bids_{}_t0'.format(volume_column), 'bids_{}_t1'.format(volume_column)], axis = 1)
    
def calculate_deltas(data_1, data_2, which = 'both', name = 'time_format', columns = ['price', 'volume']):
    
    def column_names(which, columns):
        price_column = "{}_{}".format(which, columns[0]) 
        volume_column = "{}_{}".format(which, columns[1])
        
        return price_column, volume_column
         
    
    if which == 'asks' or which == 'both':
        asks_price_column, asks_volume_column = column_names("asks", columns)
        
        asks_data_1 = data_1[[asks_price_column, asks_volume_column]]
        asks_data_2 = data_2[[asks_price_column, asks_volume_column]]
        
        merged_asks = merge_data(asks_data_1, asks_data_2, on = asks_price_column)
        merged_asks.columns = [asks_price_column, '{}_t0'.format(asks_volume_column), '{}_t1'.format(asks_volume_column)]
        
        store = merged_asks['{}_t1'.format(asks_volume_column)] - merged_asks['{}_t0'.format(asks_volume_column)]
        
        if name == 'time_format':
            merged_asks['delta_asks_{}'.format(time_delta(data_1, data_2))] = store
        elif name == 'none': 
            merged_asks['delta_asks'] = store
        
    if which == 'bids' or which == 'both':
        bids_price_column, bids_volume_column = column_names("bids", columns)
        
        bids_data_1 = data_1[[bids_price_column, bids_volume_column]]
        bids_data_2 = data_2[[bids_price_column, bids_volume_column]]
        
        merged_bids = merge_data(bids_data_1, bids_data_2, on = bids_price_column, ascending = False)
        merged_bids.columns = [bids_price_column, "{}_t0".format(bids_volume_column) , "{}_t1".format(bids_volume_column)]
        
        store = merged_bids["{}_t1".format(bids_volume_column)] - merged_bids["{}_t0".format(bids_volume_column)]
        
        if name == 'time_format':
            merged_bids['delta_bids_{}'.format(time_delta(data_1, data_2))] = store
        elif name == 'none': 
            merged_bids['delta_bids'] = store    
    
    if which == 'both':
        return merged_bids, merged_asks
    elif which == 'asks':
        return merged_asks
    elif which == 'bids':
        return merged_bids
    


def calculate_multiple_deltas(data, first, count, which = 'both', name = 'time_format', columns = ['price', 'volume']):
    
    if type(data) != list:
        raise TypeError("Input data needs to be of type list")
            
    if type(first) != int:
        raise TypeError("Input first needs to be of type int")
            
    if type(count) == list or type(count) == tuple:
        if max(count) > (len(data) - first - 1):
            raise ValueError("Not enough data to calculate delta's. Shorten t1 request")
    elif count > (len(data) - first - 1):
        raise ValueError("Not enough data to calculate delta's. Shorten t1 request")

    if type(count) == list or type(count) == tuple:
        counter = 0
        for i in count:
            if which == 'both':
                bids, asks = calculate_deltas(data[first],
                                              data[first+i],
                                              which = which,
                                              name = name,
                                              columns = ['price', 'volume'])
            elif which == 'asks':
                asks = calculate_deltas(data[first],
                                              data[first+i],
                                              which = which,
                                              name = name,
                                              columns = ['price', 'volume'])
            elif which == 'bids':
                bids = calculate_deltas(data[first],
                                              data[first+i],
                                              which = which,
                                              name = name,
                                              columns = ['price', 'volume'])
                
            if which == 'both' or which == 'bids':
                bids_price_column, bids_volume_column = column_names("bids", columns)
                if counter == 0:
                    bids_df = bids.drop('{}_t1'.format(bids_volume_column), axis = 1)
                else:
                    bids_df = clean_data(bids_df, clean_data(bids, 'bids'), on = bids_price_column, ascending = False)
                
            if which == 'both' or which == 'asks':
                asks_price_column, asks_volume_column = column_names("asks", columns)
                if counter == 0:
                    asks_df = asks.drop('{}_t1'.format(asks_volume_column), axis = 1)
                else:
                    asks_df = clean_data(asks_df, clean_data(asks, 'asks'), on = asks_price_column)
            counter = counter + 1
        return bids_df, asks_df
    else:
        return calculate_deltas(data[first], data[first+count], which = which, name = name, columns = ['price', 'volume'])