def calculate_bucket_end_points(data, bucket_size, which = 'bids', column = 'bids_price'):

	"""
	data: of type dataframe
	
	bucket_size: of type integer
	
	which: of type string. either 'bids' or 'asks'
	
	column: of type string. 
	
	returns:
	list of integer values representing bucket end points
	
	example:
	calculate_bucket_end_points(data = df, bucket_size = 5, which = 'bids', column = 'bids_price') = [100, 105, 110]
	
	"""
	import numpy as np
    
	max_val = data[column].max()
	min_val = data[column].min()
    
	if which == 'bids':
		return np.arange(max_val, min_val, (-1*bucket_size))
	elif which == 'asks':
		return np.arange(min_val, max_val, bucket_size)

def fill_data_into_buckets(data_point, bucket_end_points, which = 'bids'):

	"""
	data_point: of type integer
	
	bucket_end_points: of type list of integers
	
	which: of type string. either 'bids' or 'asks'
	
	returns:
	An integer value stating which bucket the datapoint falls into based on the inputed bucket end points
	
	example:
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
    
        
def create_buckets(data, bucket_size, which = 'bids', column = 'bids_price'):

	"""
	data: of type dataframe
	
	bucket_size: of type integer
	
	which: of type string. either 'bids' or 'asks'
	
	column: of type string. 
	
	returns:
	dataframe with a new column representing each row value in column according to which bucket it falls in.
	
	example:
	calculate_bucket_end_points(data = df, bucket_size = 5, which = 'bids', column = 'bids_price')
	
	"""
    
	bucket_end_points = calculate_bucket_end_points(data, bucket_size, which = which, column = column)
    
	data["{}_bucket".format(which)] = data[column].apply(fill_data_into_buckets, args = [bucket_end_points, which])
    
	return data
