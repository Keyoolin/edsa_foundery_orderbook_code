def augment_df(data):

    import pandas as pd
    
    melted_df = pd.melt(data)
    transposed_df = melted_df.transpose()
    
    column_list = []
    count = 0
    for i in list(transposed_df.loc['variable'].values):
        column_list.append(i + "_{}".format(count))
        count = count + 1
    transposed_df.columns = column_list
    transposed_df.drop("variable", inplace = True)
    
    return transposed_df


def create_feature_delta_buckets_pipeline(data, first, count, name = 'none', which = 'bids', bucket_size = 5, column = 'bids_price'):

    from deltas import calculate_multiple_deltas
    from calculate_buckets import create_buckets
    
    df = calculate_multiple_deltas(data, first, count, which = which, name = name)
    bucket_df = create_buckets(df, bucket_size = bucket_size, which = which, column = column)
    grouped_df = bucket_df.groupby('{}_bucket'.format(which))
    group_sum_df = grouped_df.sum()[["delta_{}".format(which)]]
    final = augment_df(group_sum_df)
    
    return final