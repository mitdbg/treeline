#!/usr/bin/env python
# coding: utf-8

# In[89]:


import numpy as np
import matplotlib.pyplot as plt
plt.rcParams["font.size"] = 14


# In[90]:


# Data input
x1 = (25, 50, 75, 100, 125, 150, 175, 200)
y1 = (9.8948,10.661,10.5342,10.6564,10.5823,10.2704,10.5971,10.5497)
perf = (10.7215, 10.7215, 10.7215, 10.7215, 10.7215, 10.7215, 10.7215, 10.7215)
base = (8.607, 8.607, 8.607, 8.607, 8.607, 8.607, 8.607, 8.607)

x2 = (10, 100, 1000, 10000, 100000)
y2 = (0.979029,5.13152,8.18957,9.13109,10.6564)


# In[92]:


fig, ax = plt.subplots(1,1,figsize=(6.65, 3))

# Powers of 10
ax.plot(x2, perf[:5], linestyle='-', marker='o', color = "#958934", label="Perfect", linewidth = 3.5, markersize = 15)
ax.plot(x2, y2, linestyle='-', marker='o',color = "#9fc55a", label = "Forecasting", linewidth = 3.5, markersize = 15)
ax.plot(x2, base[:5], linestyle='-', marker='o', color = "#397624", label="Base", linewidth = 3.5, markersize = 15)
ax.plot(x2[4], y2[4], marker='s', markersize=15, color = "#9fc55a")
ax.set_xscale('log')
ax.set_xlabel("Epoch length (insert ops)")
ax.set_ylabel('Throughput (kreq/s)')
ax.set_ylim([0, 12])


ax.legend(fancybox=False,
            edgecolor="#000",
            #fontsize="medium",
            loc="lower right",
         #bbox_to_anchor=(0.41, 1)
         )
fig.tight_layout()
plt.savefig("epoch_length_comparison1.pdf")
plt.show()
    


# In[96]:


fig, ax = plt.subplots(1,1,figsize=(6.65, 3))

# Close search
ax.plot(x1, perf, linestyle='-', marker='o', color = "#958934", label="Perfect", linewidth = 3.5, markersize = 15)
ax.plot(x1, y1, linestyle='-', marker='o', color = "#9fc55a", label="Forecasting", linewidth = 3.5, markersize = 15)
ax.plot(x1, base, linestyle='-', marker='o', color = "#397624", label="Base", linewidth = 3.5, markersize = 15)
ax.plot(x1[3], y1[3], marker='s', markersize=15, color = "#9fc55a")
ax.set_xlabel("Epoch length (thousands of insert ops)")
ax.set_ylabel('Throughput (kreq/s)')
ax.set_ylim([8, 11])

ax.legend(fancybox=False,
            edgecolor="#000",
            fontsize="medium",
            loc="lower right",
         #bbox_to_anchor=(0.41, 1)
         )
fig.tight_layout()
plt.savefig("epoch_length_comparison2.pdf")
plt.show()
