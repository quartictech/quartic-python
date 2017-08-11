
def simplify_df(df):
    """ remove columns with <= 1 unique values (or all zero/nan) """
    import numpy as np
    keep = []
    for col in df.columns:
        values = df[col].unique()
        if len(values) == 1:
            continue
        if df[col].dtype == np.float64:
            zero_or_nan = np.isnan(values) | (values == 0)
            if np.all(zero_or_nan):
                continue
        keep.append(col)
    return df[keep]
