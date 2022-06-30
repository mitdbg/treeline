#!/usr/bin/env python
# coding: utf-8

# In[15]:


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


# In[82]:


base_1 = (91.7036, 52.8868, 50.3852, 52.0655, 4.60011, 72.9772)
lru_1 = (75.0306, 50.0059, 49.0896, 39.9239, 4.56209, 63.9502)

base_4 = (348.096, 205.531, 194.772, 183.162, 17.413, 278.239)
lru_4 = (275.958, 183.85, 175.556, 136.292, 17.1675, 247.04)

base_16 = (1077.22, 664.93, 635.963, 583.132, 56.0463, 924.377)
lru_16 = (654.203, 521.812, 537.027, 377.8, 56.647, 699.298)


# In[83]:


cols = ["A", "B", "C", "D", "E", "F"]
idx = ['treeline_base', 'treeline_LRU']
data1 = [(1,1,1,1,1,1), tuple([l/b for l,b in zip(lru_1, base_1)])]
df1 = pd.DataFrame(data1, columns = cols, index = idx).T
data4 = [(1,1,1,1,1,1), tuple([l/b for l,b in zip(lru_4, base_4)])]
df4 = pd.DataFrame(data4, columns = cols, index = idx).T
data16 = [(1,1,1,1,1,1), tuple([l/b for l,b in zip(lru_16, base_16)])]
df16 = pd.DataFrame(data16, columns = cols, index = idx).T


# In[84]:


plt.rcParams.update(plt.rcParamsDefault)
plt.rcParams["font.size"] = 14

x = np.arange(len(df1.index))  # the label locations
width = 0.4  # the width of the bars

fig, ax = plt.subplots(1,3,figsize=(9, 3))
rects1 = ax[0].bar(x - width/2, df1["treeline_base"], width, label="Clock + Priorities", color = "#397624")
rects2 = ax[0].bar(x + width/2, df1["treeline_LRU"], width, label="LRU", color = "#9fc55a")

rects3 = ax[1].bar(x - width/2, df4["treeline_base"], width, label="Clock + Priorities", color = "#397624")
rects4 = ax[1].bar(x + width/2, df4["treeline_LRU"], width, label="LRU", color = "#9fc55a")

rects5 = ax[2].bar(x - width/2, df16["treeline_base"], width, label="Clock + Priorities", color = "#397624")
rects6 = ax[2].bar(x + width/2, df16["treeline_LRU"], width, label="LRU", color = "#9fc55a")


# Add some text for labels, title and custom x-axis tick labels, etc.
ax[0].set_ylabel('Relative Throughput')
ax[0].set_xticks(list(x))
ax[1].set_xticks(list(x))
ax[2].set_xticks(list(x))

ax[0].set_xticklabels(df1.index)
ax[1].set_xticklabels(df1.index)
ax[2].set_xticklabels(df1.index)

ax[0].grid(False)
ax[1].grid(False)
ax[2].grid(False)

ax[0].set_ylim((0, 1.5))
ax[1].set_ylim((0, 1.5))
ax[2].set_ylim((0, 1.5))

ax[0].set_xlabel("1 thread")
ax[1].set_xlabel("4 threads")
ax[2].set_xlabel("16 threads")

ax[0].spines['bottom'].set_color('0')
ax[0].spines['top'].set_color('0')
ax[0].spines['right'].set_color('0')
ax[0].spines['left'].set_color('0')

ax[0].legend(fancybox=False,
            edgecolor="#000",
            fontsize="small",
            loc="upper left",
         #bbox_to_anchor=(0.41, 1)
         )



fig.tight_layout()

plt.savefig("lru_comparison.pdf")
plt.show()


# In[ ]:




