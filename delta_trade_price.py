def get_trade_price(snapshot_1, bid_column = 'bids_price', ask_column = "asks_price"):
    return (snapshot_1[bid_column][0] + snapshot_1[ask_column][0])/2
    
def get_trade_price_change(snapshot_1, snapshot_2, bid_column = 'bids_price', ask_column = "asks_price"):
    snapshot_1_price = get_trade_price(snapshot_1, bid_column = bid_column, ask_column = ask_column)
    snapshot_2_price = get_trade_price(snapshot_2, bid_column = bid_column, ask_column = ask_column)
    
    return snapshot_2_price - snapshot_1_price
    
    
def trade_price_change_classifier(snapshot_1, snapshot_2, bid_column = 'bids_price', ask_column = "asks_price", cutoff = 0):
    change = get_trade_price_change(snapshot_1, snapshot_2, bid_column = bid_column, ask_column = ask_column)
    
    if change > cutoff:
        return 1
    elif change < (-1 * cutoff):
        return -1
    else: 
        return 0

def delta_trade_price_final_classifier(data, start, end, increment, cutoff = 0, bid_column = 'bids_price', ask_column = "asks_price"):
    
    """
    Pipeline to classify the change in the trade price between snapshots as either up, down or no change.
    
    Parameters
    ----------
    
        data : list, required
        List of snapshots saved as dataframes
        
        start : int, required
        The initial start point in the list of snapshots for which to classify trade price change
        
        end : int, required
        The end point in the list of snapshots for which to classify trade price change
        
        increment : int, required
        The future step value for which to base calculations on. 
        E.g. 5, would return the difference between T25 and T30.
        
        cutoff : int, optional
        The range of which no change is classified.
        All values within the range of 0 +-cutoff will be classified as no change
        
        bid_column : string, optional
        The name of the bids column in the list of dataframes
        
        ask_column : string, optional
        The name of the ask columns in the list of dataframes

    Returns
    ----------
        delta_price : Dataframe
        Dataframe of shape (1, x). Effectively a row vector
        
    """
    import pandas as pd
    
    delta_price = []
    while ((start + increment) <= len(data)) & (start != (end + 1)):
        snapshot_1 = data[start]
        snapshot_2 = data[start+increment]
        delta_price.append(("{}_{}".format(start, start+increment),trade_price_change_classifier(snapshot_1,
                                                         snapshot_2,
                                                         bid_column = bid_column,
                                                         ask_column = ask_column,
                                                         cutoff = cutoff)))
        start = start + 1
        
    return pd.DataFrame(delta_price, columns = ["between", "change"]).set_index("between")
   