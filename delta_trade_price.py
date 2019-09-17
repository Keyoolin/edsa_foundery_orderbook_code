def get_trade_price(snapshot_1, bid_column = 'bids_price', ask_column = "asks_price"):
    return (snapshot_1[bid_column][0] + snapshot_1[ask_column][0])/2
    
def get_trade_price_change(snapshot_1, snapshot_2, bid_column = 'bids_price', ask_column = "asks_price"):
    snapshot_1_price = get_trade_price(snapshot_1, bid_column = bid_column, ask_column = ask_column)
    snapshot_2_price = get_trade_price(snapshot_2, bid_column = bid_column, ask_column = ask_column)
    
    return snapshot_2_price - snapshot_1_price
    
    
def trade_price_change_classifier(snapshot_1, snapshot_2, bid_column = 'bids_price', ask_column = "asks_price"):
    change = get_trade_price_change(snapshot_1, snapshot_2, bid_column = bid_column, ask_column = ask_column)
    
    if change > 0:
        return 1
    elif change == 0:
        return 0
    elif change < 0:
        return -1
        
        
def delta_trade_price_final_classifier(data, increment, bid_column = 'bids_price', ask_column = "asks_price"):
    start = 0
    delta_price = []
    while (start + increment) < len(data):
        snapshot_1 = data[start]
        snapshot_2 = data[start+increment]
        delta_price.append(trade_price_change_classifier(snapshot_1,
                                                         snapshot_2,
                                                         bid_column = bid_column,
                                                         ask_column = ask_column))
        start = start + 1
    return delta_price