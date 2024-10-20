import networkx as nx
from fuzzywuzzy import fuzz
import glob
import pandas as pd
import time
import os

start_time = time.time()

# Blok try-except untuk pembacaan file CSV
try:
    # Mencari file dengan pola "Third*.csv"
    filename = str(glob.glob(r"C:\Dimas\Docs\Me\Coding\Algoritma Bootcamp\Material\Capstone\DCD\Python\Third*.csv"))[2:-2]
    if filename:  # Jika file ditemukan
        df = pd.read_csv(filename, encoding='latin1', on_bad_lines='skip', low_memory=False)
        df = df.rename(columns={'Buyer_User_ID': 'buyer_id'})
        print('Opening ' + filename)
    else:
        raise FileNotFoundError("CSV file tidak ditemukan.")
except FileNotFoundError as e:
    print(f"Error: {e}")
    raise

# Proses fuzzy matching
df['shipping_address_x'] = df['shipping_address']
df['shipping_address_z'] = df['shipping_address']  # Make sure 'shipping_address_z' is created

df_list = [d for _, d in df.groupby(['shipping_city'])]

newdata = []
for d in df_list:
    for category in d.shipping_address:
        for master_category in d.shipping_address:
            if fuzz.token_set_ratio(category, master_category) >= 80:
                newdata.append({"dish1": category, "dish2": master_category})
                break

newdf = pd.DataFrame(newdata, columns=['dish1', 'dish2'])
newdf = newdf.rename(columns={"dish1": "shipping_address_x", "dish2": "shipping_address_z"})

# Merging new fuzzy data with original DataFrame
newdf2 = pd.merge(newdf, df[['order_id', 'order_sn', 'buyer_id', 'shipping_address', 'shipping_address_x', 'shipping_address_z']], 
                  how="left", on="shipping_address_x")
newdf2 = newdf2.drop_duplicates()

# Ensure the second merge on 'shipping_address_z' is valid
if 'shipping_address_z' in df.columns:
    newdf2 = pd.merge(newdf2, df[['order_id', 'order_sn', 'buyer_id', 'shipping_address', 'shipping_address_x', 'shipping_address_z']], 
                      how="left", on="shipping_address_z")
    newdf2 = newdf2.drop_duplicates()
else:
    print("Error: 'shipping_address_z' is missing from the DataFrame.")

# Clean up columns
newdf2 = newdf2.drop(columns=['order_id_x', 'shipping_address_x', 'shipping_address_z_y',
                              'order_id_y', 'shipping_address_y', 'shipping_address_x_y'])

# Graph-based connected component grouping
G = nx.from_pandas_edgelist(newdf2, 'buyer_id_x', 'buyer_id_y')
g = nx.connected_components(G)

# Fix for DeprecationWarning (pd.Series without data)
S = pd.Series(dtype=object)
for i, n in enumerate(g):
    s = pd.Series(sorted(list(n)), index=[i] * len(n))
    S = pd.concat([S, s])

result = pd.DataFrame(S)
result = result.rename(columns={0: 'buyer_id_x'})
result['group_number'] = S.index

# Merge the result back with the DataFrame
newdf3 = pd.merge(newdf2, result, how="left", on="buyer_id_x")

# Pivot table to calculate unique buyer counts (group size)
pivot1 = newdf3.pivot_table(values='buyer_id_x', index='group_number', aggfunc=pd.Series.nunique)
pivot1 = pivot1.rename(columns={"buyer_id_x": "group_size"})

# Merge group size back into newdf3
newdf3 = pd.merge(newdf3, pivot1, how="left", on="group_number")
newdf3 = newdf3.rename(columns={"buyer_id_x_x": "buyer_userid_x"})

# Drop unnecessary columns
newdf3 = newdf3.drop(columns=['buyer_id_y', 'buyer_address_y'])

# Filter and rename columns
newdf4 = newdf3.drop_duplicates(subset=['buyer_userid_x'], keep='first')
newdf4 = newdf4[newdf4['group_size'] >= 2]
newdf4 = newdf4.rename(columns={"order_sn_x": "order_sn", "group_number": "fuzzy_group_number", "group_size": "fuzzy_group_size"})

# Merge fuzzy matching results back into the original DataFrame
df = df.merge(newdf4[['order_sn', 'fuzzy_group_number', 'fuzzy_group_size']], on='order_sn', how='left')

# Construct a valid file path and save the result
output_filename = 'ResultFuzzy2_' + os.path.basename(filename).split('.')[0] + ".xlsx"
df.to_excel(output_filename, index=False)

print("Fuzzy Wuzzy Successfully")
print("Process finished --- %s seconds ---" % (time.time() - start_time))
