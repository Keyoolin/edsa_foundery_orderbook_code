def merge_data(data_1, data_2, on, ascending = True):
    df = data_1.merge(data_2, on = on, how = 'outer')
    df = df.sort_values(by = on, ascending = ascending)
    df = df.fillna(0)
    return df

def time_delta(data_1, data_2):
    return data_2.iloc[1].timestamp - data_1.iloc[1].timestamp
    
def calculate_deltas(data_1, data_2, which = 'both', name = 'time_format'):
        
    if which == 'asks' or which == 'both':
        asks_data_1 = data_1[['asks_price', 'asks_volume']]
        asks_data_2 = data_2[['asks_price', 'asks_volume']]
        
        merged_asks = merge_data(asks_data_1, asks_data_2, on = 'asks_price')
        merged_asks.columns = ['asks_price', 'asks_volume_t0', 'asks_volume_t1']
        
        store = merged_asks['asks_volume_t1'] - merged_asks['asks_volume_t0']
        
        if name == 'time_format':
            merged_asks['delta_asks_{}'.format(time_delta(data_1, data_2))] = store
        elif name == 'none': 
            merged_asks['delta_asks'] = store
        
    if which == 'bids' or which == 'both':
        bids_data_1 = data_1[['bids_price', 'bids_volume']]
        bids_data_2 = data_2[['bids_price', 'bids_volume']]
        
        merged_bids = merge_data(bids_data_1, bids_data_2, on = 'bids_price', ascending = False)
        merged_bids.columns = ['bids_price', 'bids_volume_t0', 'bids_volume_t1']
        
        store = merged_bids['bids_volume_t1'] - merged_bids['bids_volume_t0']
        
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
    
def merged_data(data, which):
    if which == 'asks':
        return data.drop(['asks_volume_t0', 'asks_volume_t1'], axis = 1)
    elif which == 'bids':
        return data.drop(['bids_volume_t0', 'bids_volume_t1'], axis = 1)

def calculate_multiple_deltas(data, first, count, which = 'both', name = 'time_format'):
    
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
            bids, asks = calculate_deltas(data[first], data[first+i], which = which, name = name)
            if counter == 0:
                bids_df = bids.drop('bids_volume_t1', axis = 1) #merged_data(bids, 'bids')
                asks_df = asks.drop('asks_volume_t1', axis = 1) #merged_data(asks, 'asks')
            else:
                bids_df = merge_data(bids_df, merged_data(bids, 'bids'), on = "bids_price", ascending = False)
                asks_df = merge_data(asks_df, merged_data(asks, 'asks'), on = "asks_price")
            counter = counter + 1
        return bids_df, asks_df
    else:
        return calculate_deltas(data[first], data[first+count], which = which, name = name)