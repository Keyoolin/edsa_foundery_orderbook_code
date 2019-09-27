def augment_df(data, first, count):

    import pandas as pd
    
    melted_df = pd.melt(data)
    transposed_df = melted_df.transpose()
    
    column_list = []
    counter = 0
    for i in list(transposed_df.loc['variable'].values):
        column_list.append(i + "_{}".format(counter))
        counter = counter + 1
    transposed_df.columns = column_list
    transposed_df.drop("variable", inplace = True)
    
    transposed_df["first"] = first
    transposed_df["last"] = first+count
    transposed_df["between"] = '{}_{}'.format(first, (first+count))
    
    return transposed_df


def create_feature_delta_buckets_pipeline(data, first, count, name = 'none', which = 'bids', bucket_size = 5, columns = ['price', 'volume']):


    """
    Pipeline to calculate deltas between buckets in relative terms
    
    Parameters
    ----------
    
        data : list, required
        List of snapshots saved as dataframes
        
        first : int, required
        The initial start point in the list of snapshots for which to calculate deltas
        
        count : int, list or tuple, required
        How many snapshots in the future should the delta's be calculated for?
        if list is used, calculates multiple deltas. e.g. between t0 and t2, and t0 and t3
        
        name : str, "none" | "time_format", optional, default = 'time_format'
        Selects the naming format of the returned delta column. 
        Time_format calculates the time difference and appends it to the column name.
        None names it a delta column.
        
        which : str, "asks" | "bids", optional, default = 'bids'
        Which deltas to calculate, bids or asks
        
        bucket_size : int, optional, default = 5
        Dollar bucket size to be created of prices in each snapshot
        
        columns : list, optional, default = ['price', 'volume'] 
        Naming convention of the price and volume column.
        Column names must be in the format "{1}_{2}", where 1 is the which parameter
        and 2 is the column name parameter.
        
        
    Returns
    ----------
        final : Dataframe
        Dataframe of shape (1, x). Effectively a row vector
        
    """

    from deltas import calculate_multiple_deltas
    from calculate_buckets import create_buckets
    
    df = calculate_multiple_deltas(data = data, first = first, count = count, which = which, name = name, columns = columns)
    bucket_df = create_buckets(snapshot = df, bucket_size = bucket_size, which = which, columns = columns)
    grouped_df = bucket_df.groupby('{}_bucket'.format(which))
    group_sum_df = grouped_df.sum()[["delta_{}".format(which)]]
    single_feature_vector = augment_df(group_sum_df, first, count)
    
    return single_feature_vector

def bucket_feature_final(data, first = 0, count = 1, name = 'none', which = 'asks', bucket_size = 5, columns = ['price','volume']):

    """
    Pipeline to calculate deltas between buckets in relative terms over multiple snapshots in time
    
    Parameters
    ----------
    
        data : list, required
        List of snapshots saved as dataframes
        
        first : int, required
        The initial start point in the list of snapshots for which to calculate deltas
        
        count : int, list or tuple, required
        How many snapshots in the future should the delta's be calculated for?
        if list is used, calculates multiple deltas. e.g. between t0 and t2, and t0 and t3
        
        name : str, "none" | "time_format", optional, default = 'time_format'
        Selects the naming format of the returned delta column. 
        Time_format calculates the time difference and appends it to the column name.
        None names it a delta column.
        
        which : str, "asks" | "bids", optional, default = 'asks'
        Which deltas to calculate, bids or asks
        
        bucket_size : int, optional, default = 5
        Dollar bucket size to be created of prices in each snapshot
        
        columns : list, optional, default = ['price', 'volume'] 
        Naming convention of the price and volume column.
        Column names must be in the format "{1}_{2}", where 1 is the which parameter
        and 2 is the column name parameter.
        
        
    Returns
    ----------
        final : Dataframe
        Dataframe of changes in the relative buckets of each snapshot with a future snapshot.
        
    """
    
    import pandas as pd
    
    delta_bucket_feature_vector = create_feature_delta_buckets_pipeline(data = data,
                                                                        first = first,
                                                                        count = count,
                                                                        name = name,
                                                                        which = which,
                                                                        bucket_size = bucket_size,
                                                                        columns = columns)
    
    for i in range(len(data)-first-2):
        df_temp = create_feature_delta_buckets_pipeline(data = data,
                                                        first = first+i+1,
                                                        count = count,
                                                        name = name,
                                                        which = which,
                                                        bucket_size = bucket_size,
                                                        columns = columns)
                                                        
        delta_bucket_feature_vector = pd.concat([delta_bucket_feature_vector,
                                                 df_temp], sort = False)
        
    delta_bucket_feature_vector.reset_index(inplace = True)
    delta_bucket_feature_vector.drop("index",
                                     axis = 1,
                                     inplace = True)
    
    delta_bucket_feature_vector.set_index('between', inplace=True)
    
    delta_bucket_feature_vector.fillna(0, inplace = True)
    
    return delta_bucket_feature_vector

def combine_vector(data, first, end):
    
    """
    Takes a dataframe containing multiple row vectors of deltas and 
    returns a single row vector containing the deltas.
    
    Parameters
    ----------
    
        data : DataFrame, required
        Dataframe containing multiple delta_buckets
        
        first : int, required
        The starting row vector to be combined into a single row vector.
        All row vectors from this starting vector, 
        till the end vector will be combined into a single row vector

        end : int, required
        The ending row vector to be combined into a single row vector.
        All row vectors before and including this end vector, 
        from the starting vector will be combined into a single row vector
        
    Returns
    ----------
        data : Dataframe
        Dataframe of shape (1, x). Effectively a row vector
        
    """
        
    data = data[(data['first'] >= first) & (data['last'] <= end)].copy()
    data.reset_index(inplace = True)
    data.drop(["first", "last", "between"], axis = 1, inplace = True)
    data = data.unstack().to_frame().T
    data.index = ['{}_{}'.format(first, end)]

    return data

def create_delta_feature(data, length):
    
    """
    Takes a dataframe containing multiple row vectors of deltas and 
    combines them into row vectors of the desired size.
    
    Parameters
    ----------
    
        data : DataFrame, required
        Dataframe containing multiple delta_buckets
        
        length : int, required
        Length of the vectors required. 
        For example: if 10 deltas are to be considered, input 10.
        
    Returns
    ----------
        data : Dataframe
        Dataframe of row vectors containing the required deltas
        
    """    
    
    i = 0
    df = combine_vector(data, i, length)
    
    while i + length != len(data):
        i = i + 1
        df_temp = combine_vector(data, i, length+i)
        df = pd.concat([df, df_temp], sort = False)
        
    df.fillna(0, inplace = True)
    
    return df