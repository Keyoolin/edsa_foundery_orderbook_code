def initialise_dataframes(files, directory):
    
    def read(csv):
        
        import pandas as pd
        from datetime import datetime
        
        df = pd.read_csv(csv)
        df.timestamp = pd.to_datetime(df.timestamp)
        return df
    
    if type(files) == list:
        try:
            data = []
            for i in files:
                if 'order_book' in i and '.csv' in i:
                    data.append(read((directory + i)))
                    
        except:
            raise TypeError("List must contain reference to a specific csv in the file location")
                
    elif type(files) == str and '.csv' in i:
        return read(files)
    
    else: 
        raise TypeError("Input variable must be of type string or list of strings referring to a csv in file location")
                
    return data