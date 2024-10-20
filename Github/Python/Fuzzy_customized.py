import networkx as nx
from fuzzywuzzy import fuzz
import glob
import pandas as pd
import os
import time

start_time = time.time()

# Coba untuk membuka file CSV terlebih dahulu, jika tidak ada, buka file Excel
csv_files = glob.glob("./*.csv")
xlsx_files = glob.glob("./*.xlsx")

if csv_files:
    filename = csv_files[0]
    df = pd.read_csv(filename)
    print(f'Opening {filename}')
elif xlsx_files:
    filename = xlsx_files[0]
    df = pd.read_excel(filename)
    print(f'Opening {filename}')
else:
    raise FileNotFoundError("Tidak ada file CSV atau Excel yang ditemukan di direktori saat ini.")

# Menghapus file setelah digunakan
if os.path.exists(filename):
    os.remove(filename)

# Proses fuzzy logic
df = df.rename(columns={'Buyer_User_ID': 'buyer_id'})
df['shipping_address_x'] = df['shipping_address']
df['shipping_address_z'] = df['shipping_address']
df_list = [d for _, d in df.groupby(['shipping_city'])]

newdata = []
for d in df_list:
    for category in d.shipping_address:
        for master_category in d.shipping_address:
            if category != master_category and fuzz.token_set_ratio(category, master_category) >= 90:  # default 80
                newdata.append({"shipping_address_x": category, "shipping_address_z": master_category})
                break

newdf = pd.DataFrame(newdata)

# Merge DataFrame
newdf2 = pd.merge(newdf, df[['order_id', 'order_sn', 'buyer_id', 'shipping_address']], 
                  how="left", on="shipping_address_x")

newdf2 = newdf2.drop_duplicates()

# Lakukan merge kedua berdasarkan shipping_address_z
newdf2 = pd.merge(newdf2, df[['order_id', 'order_sn', 'buyer_id', 'shipping_address']], 
                  how="left", left_on="shipping_address_z", right_on="shipping_address", suffixes=('_x', '_y'))

# Buat graph menggunakan networkx
G = nx.from_pandas_edgelist(newdf2, 'buyer_id_x', 'buyer_id_y')
connected_components = nx.connected_components(G)

S = pd.Series(dtype=object)
for i, component in enumerate(connected_components):
    s = pd.Series(sorted(list(component)), index=[i]*len(component))
    S = pd.concat([S, s])

result = pd.DataFrame(S)
result = result.rename(columns={0: 'buyer_id_x'})
result['group_number'] = S.index

# Merge hasil clustering dengan DataFrame utama
newdf3 = pd.merge(newdf2, result, how="left", on="buyer_id_x")

# Buat pivot untuk menghitung jumlah unik buyer_id di setiap grup
pivot1 = newdf3.pivot_table(values='buyer_id_x', index='group_number', aggfunc=pd.Series.nunique)
newdf3 = pd.merge(newdf3, pivot1, how="left", on="group_number")
newdf3 = newdf3.rename(columns={"buyer_id_x_y": "group_size", "buyer_id_x_x": "buyer_userid_x"})

# Simpan hasil ke file Excel
newdf4 = newdf3.drop_duplicates(subset=['buyer_userid_x'], keep='first')
newdf4 = newdf4[(newdf4.group_size >= 2)]
newdf4 = newdf4.rename(columns={"order_sn_x": "order_sn", 
                                "group_number": "fuzzy_group_number", 
                                "group_size": "fuzzy_group_size"})

df = df.merge(newdf4[['order_sn', 'fuzzy_group_number', 'fuzzy_group_size']], on='order_sn', how='left')

final_dataframe = df[['buyer_id', 'fuzzy_group_number', 'fuzzy_group_size']]
final_dataframe.to_excel('.././RESULT_Fuzzy.xlsx', index=False)

print("Fuzzy Wuzzy Successfully")
print("Process finished --- %s seconds ---" % (time.time() - start_time))
