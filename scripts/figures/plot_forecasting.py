#!/usr/bin/env python
# coding: utf-8

# In[19]:


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import gmean


# In[4]:


cols = ["64 B", "512 B"]
idx = ['treeline_base', 'treeline_forecasting', 'treeline_perfect','rocksdb' ]
data1 = [(9.13726, 7.37666), (11.2312,8.74956), (11.4505, 10.8392), (17.177,  19.593)]
data8 = [(60.4006, 18.8215), (91.1283,29.9629), (101.052, 88.5728), ( 98.796,  92.909)]
df1 = pd.DataFrame(data1, columns = cols, index = idx).T
df8 = pd.DataFrame(data8, columns = cols, index = idx).T


# In[8]:


plt.rcParams.update(plt.rcParamsDefault)
plt.rcParams["font.size"] = 14

x = np.arange(len(df1.index))  # the label locations
width = 0.3  # the width of the bars

fig, ax = plt.subplots(figsize=(3, 2.25))
rects1 = ax.bar(x - width, df1["treeline_base"], width, label="No Forecasting", color = "#9fc55a")
rects2 = ax.bar(x, df1["treeline_forecasting"], width, label="Forecasting", color = "#397624")
rects3 = ax.bar(x + width, df1["treeline_perfect"], width, label="Perfect", color = "#958934")
#rects4 = ax.bar(x + 3*width/2, df1["rocksdb"], width, label="rocksdb", color = "#875826")

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Throughput (kreq/s)')
ax.set_xticks(list(x))
ax.set_xticklabels(df1.index)
ax.grid(False)
ax.set_ylim((0, 27))


ax.spines['bottom'].set_color('0')
ax.spines['top'].set_color('0')
ax.spines['right'].set_color('0')
ax.spines['left'].set_color('0')

ax.legend(fancybox=False,
            edgecolor="#000",
            fontsize="small",
            loc="upper left",
         #bbox_to_anchor=(0.41, 1)
         )

fig.tight_layout()

plt.savefig("1.pdf")
plt.show()


# In[7]:


plt.rcParams.update(plt.rcParamsDefault)
plt.rcParams["font.size"] = 14

x = np.arange(len(df8.index))  # the label locations
width = 0.3  # the width of the bars

fig, ax = plt.subplots(figsize=(3, 2.25))
rects1 = ax.bar(x - width, df8["treeline_base"], width, label="No Forecasting", color = "#9fc55a")
rects2 = ax.bar(x , df8["treeline_forecasting"], width, label="Forecasting", color = "#397624")
rects3 = ax.bar(x + width, df8["treeline_perfect"], width, label="Perfect", color = "#958934")
#rects4 = ax.bar(x + 3*width/2, df8["rocksdb"], width, label="rocksdb", color = "#875826")

# Add some text for labels, title and custom x-axis tick labels, etc.

ax.set_xticks(list(x))
ax.set_xticklabels(df8.index)
ax.grid(False)
ax.set_ylim((0, 105))


ax.spines['bottom'].set_color('0')
ax.spines['top'].set_color('0')
ax.spines['right'].set_color('0')
ax.spines['left'].set_color('0')


fig.tight_layout()

plt.savefig("8.pdf")
plt.show()


# In[21]:


gap_closed_64B_1 = ((df1["treeline_forecasting"][0] - df1["treeline_base"][0]) / 
                    (df1["treeline_perfect"][0] - df1["treeline_base"][0]))
gap_closed_64B_8 = ((df8["treeline_forecasting"][0] - df8["treeline_base"][0]) / 
                    (df8["treeline_perfect"][0] - df8["treeline_base"][0]))
avg_gap_closed_64B = gmean((gap_closed_64B_1, gap_closed_64B_8))
avg_gap_closed_64B


# In[22]:


imp_64B_1 = df1["treeline_forecasting"][0] / df1["treeline_base"][0]
imp_64B_8 = df8["treeline_forecasting"][0] / df8["treeline_base"][0]
imp_512B_1 = df1["treeline_forecasting"][1] / df1["treeline_base"][1]
imp_512B_8 = df8["treeline_forecasting"][1] / df8["treeline_base"][1]
avg_imp = gmean((imp_512B_1, imp_512B_8, imp_64B_1, imp_64B_8))
avg_imp


# In[24]:


colsr = ["64 B", "512 B"]
idxr = ['treeline_base', 'treeline_forecasting']
reorgs1 = [(13366,150814), (3004,75275)]
reorgs8 = [(25723,330849), (6126,229663)]
dfr1 = pd.DataFrame(reorgs1, columns = colsr, index = idxr).T
dfr8 = pd.DataFrame(reorgs8, columns = colsr, index = idxr).T


# In[26]:


reorg_red_64B_1 = 1 - (dfr1["treeline_forecasting"][0] / dfr1["treeline_base"][0])
reorg_red_64B_8 = 1 - (dfr8["treeline_forecasting"][0] / dfr8["treeline_base"][0])
reorg_red_512B_1 = 1 - (dfr1["treeline_forecasting"][1] / dfr1["treeline_base"][1])
reorg_red_512B_8 = 1 - (dfr8["treeline_forecasting"][1] / dfr8["treeline_base"][1])


# In[28]:


avg_reorg_red_64B = gmean((reorg_red_64B_1, reorg_red_64B_8))
avg_reorg_red_64B


# In[29]:


avg_reorg_red = gmean((reorg_red_64B_1, reorg_red_64B_8, reorg_red_512B_8, reorg_red_512B_1))
avg_reorg_red


# In[ ]:




