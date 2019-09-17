def calculate_bucket_end_points(snapshot, bucket_size, which = 'bids', column = ['price', 'volume']):

	"""
    Calculated the bucket end points for a given snapshot based on the price and the bucket_size
    
    Parameters
    ----------
	snapshot : dataframe, required
    The snapshot for which buckets should be calculated
	
	bucket_size : int, required
    The dollar size of the bucket
	
	which : str, "asks" | "bids", optional, default = 'bids'
    Which deltas to calculate, bids or asks
	
	column : list, optional, default = ['price', 'volume'] 
    Naming convention of the price and volume column.
    Column names must be in the format "{1}_{2}", where 1 is the which parameter
    and 2 is the column name parameter. 
	
	Returns
    ----------
	list of integer values representing bucket end points
	
	Example
    ----------
	calculate_bucket_end_points(snapshot = df, bucket_size = 5, which = 'bids', column = 'bids_price') = [100, 105, 110]
	
	"""
    
	import numpy as np
    from deltas import column_names
    
    price_column, volume_column = column_names(which, columns)
    
	max_val = snapshot[price_column].max()
	min_val = snapshot[price_column].min()
    
	if which == 'bids':
		return np.arange(max_val, min_val, (-1*bucket_size))
	elif which == 'asks':
		return np.arange(min_val, max_val, bucket_size)

def fill_data_into_buckets(data_point, bucket_end_points, which = 'bids'):

	"""
    Categorises a data point based on which bucket it falls into as per the bucket end points
    
    Parameters
    ----------
	data_point : int, required
    The data_point to be filled into buckets
	
	bucket_end_points : list, required
    List of integers representing the bucket end values
	
	which : str, "asks" | "bids", optional, default = 'bids'
    Which deltas to calculate, bids or asks
	
	Returns
    ---------
	An integer value stating which bucket the datapoint falls into based on the inputed bucket end points
	
	Example
    ---------
	fill_data_into_buckets(data_point = 100, bucket_end_points = [108, 103, 98], which = 'bids') = 1
	
	"""

	for i in range(len(bucket_end_points)):
		if i != (len(bucket_end_points) - 1):
			if which == 'bids':
				if (data_point <= bucket_end_points[i]) & (data_point > bucket_end_points[i + 1]):
					return i
			elif which == 'asks':
				if (data_point >= bucket_end_points[i]) & (data_point < bucket_end_points[i + 1]):
					return i
		else: 
			return i
    
        
def create_buckets(snapshot, bucket_size, which = 'bids', column = ['price', 'volume']):

	"""
    Pipeline function to create buckets and fill data into the buckets
    
    Parameters
    ----------
	snapshot : Dataframe, required
    The snapshot for which buckets must be created
	
	bucket_size : int, required
    The dollar size of the bucket
	
	which : str, "asks" | "bids", optional, default = 'bids'
    Which deltas to calculate, bids or asks
	
	column : list, optional, default = ['price', 'volume'] 
    Naming convention of the price and volume column.
    Column names must be in the format "{1}_{2}", where 1 is the which parameter
    and 2 is the column name parameter.  
	
	Returns
    ----------
	Dataframe with a new column representing each row value in column according to which bucket it falls in.
	
	Example
    ----------
	calculate_bucket_end_points(snapshot = df, bucket_size = 5, which = 'bids', column = 'bids_price')
	
	"""
    from deltas import column_names
    price_column, volume_column = column_names(which, columns)
    
	bucket_end_points = calculate_bucket_end_points(snapshot, bucket_size, which = which, column = price_column)
    
	snapshot["{}_bucket".format(which)] = snapshot[price_column].apply(fill_data_into_buckets, args = [bucket_end_points, which])
    
	return snapshot
