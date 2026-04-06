def buggy_transformation(df):
   
    import pdb; pdb.set_trace()  
    df["fare_dollars"] = df["fare_amount"] * 100   
    top_fares = df.nlargest(11, "fare_amount")    
    cheap_trips = df[df["fare_amount"] > 10]
    return top_fares, cheap_trips