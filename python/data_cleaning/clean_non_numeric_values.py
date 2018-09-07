#clean non numeric values from a column
df.loc[:, 'width'] = pd.to_numeric(df['width'], errors='coerce')