#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


# In[6]:


cols = ["64 B", "512 B"]
idx = ['treeline_base', 'treeline_forecasting', 'treeline_perfect','rocksdb' ]
data1 = [(8.778, 6.581), (10.682,7.660), (10.722, 10.491), (17.177,  19.593)]
data8 = [(53.688, 21.271), (73.147,27.628), (92.501, 63.912), ( 98.796,  92.909)]
df1 = pd.DataFrame(data1, columns = cols, index = idx).T
df8 = pd.DataFrame(data8, columns = cols, index = idx).T


# In[7]:


plt.rcParams.update(plt.rcParamsDefault)
plt.rcParams["font.size"] = 14

x = np.arange(len(df1.index))  # the label locations
width = 0.3  # the width of the bars

fig, ax = plt.subplots(figsize=(3, 2.25))
rects1 = ax.bar(x - width, df1["treeline_base"], width, label="Base", color = "#397624")
rects2 = ax.bar(x, df1["treeline_forecasting"], width, label="Forecasting", color = "#9fc55a")
rects3 = ax.bar(x + width, df1["treeline_perfect"], width, label="Perfect", color = "#958934")
#rects4 = ax.bar(x + 3*width/2, df1["rocksdb"], width, label="rocksdb", color = "#875826")

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Throughput (kreq/s)')
ax.set_xticks(list(x))
ax.set_xticklabels(df1.index)
ax.grid(False)
ax.set_ylim((0, 25))


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


# In[8]:


plt.rcParams.update(plt.rcParamsDefault)
plt.rcParams["font.size"] = 14

x = np.arange(len(df8.index))  # the label locations
width = 0.3  # the width of the bars

fig, ax = plt.subplots(figsize=(3, 2.25))
rects1 = ax.bar(x - width, df8["treeline_base"], width, label="Base", color = "#397624")
rects2 = ax.bar(x , df8["treeline_forecasting"], width, label="Forecasting", color = "#9fc55a")
rects3 = ax.bar(x + width, df8["treeline_perfect"], width, label="Perfect", color = "#958934")
#rects4 = ax.bar(x + 3*width/2, df8["rocksdb"], width, label="rocksdb", color = "#875826")

# Add some text for labels, title and custom x-axis tick labels, etc.

ax.set_xticks(list(x))
ax.set_xticklabels(df8.index)
ax.grid(False)
ax.set_ylim((0, 100))


ax.spines['bottom'].set_color('0')
ax.spines['top'].set_color('0')
ax.spines['right'].set_color('0')
ax.spines['left'].set_color('0')


fig.tight_layout()

plt.savefig("8.pdf")
plt.show()


# In[ ]:




