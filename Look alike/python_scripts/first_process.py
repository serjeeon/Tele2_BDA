"""
Processing hashed msisdn.

There are three files. Two of them should be combined and
intersected with the third one.

"""
import pandas as pd

df1 = pd.read_csv('taxonomy/tc9d90f2.csv', header=None)
df2 = pd.read_csv('taxonomy/t7ee4dcf.csv', header=None)
df3 = pd.read_csv('taxonomy/t678cb3d.csv', header=None)
df2 = df2.append(df3)

df1.columns = ['hash']
df2.columns = ['hash']

s1 = set(df1['hash'].values)
s2 = set(df2['hash'].values)
segment_msisdn = list(s1.intersection(s2))

with open('segment_msisdn.txt', 'w') as f:
    for i in segment_msisdn:
        f.write(i + '\n')