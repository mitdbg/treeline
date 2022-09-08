#!/usr/bin/env python
# coding: utf-8

# In[3]:


import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable

plt.rcParams["font.size"] = 14


# In[4]:


# Data input
x = (10, 100, 1000, 10000, 
     25000, 50000, 75000, 100000, 
     125000, 150000, 175000, 200000)
        
y = (1.43976,6.0383,8.77653,9.98891,
           10.7142,10.8255,10.7379,11.2312,
           10.9765,10.8194, 10.5838, 10.6265)

perf = 11.4505
base = 9.13726


# In[5]:


fig, ax = plt.subplots(1,1,figsize=(6.65, 3))


axMain = plt.subplot(111)
axMain.plot(x[0], perf, linestyle=':', marker='o', color = "#958934", label="Perfect", linewidth = 3.5, markersize = 0)
axMain.plot(x[4:], y[4:], linestyle='-', marker='o',color = "#397624", label = "Forecasting", linewidth = 3.5, markersize = 15)
axMain.plot(x[0], base, linestyle=':', marker='o', color = "#9fc55a", label="No Forecasting", linewidth = 3.5, markersize = 0)
axMain.plot(x[7], y[7], marker='s', markersize=15, color = "#397624")
axMain.set_xscale('linear')
axMain.set_xlim((25000, 225000))
axMain.spines['left'].set_visible(True)
axMain.yaxis.set_ticks_position('right')
axMain.yaxis.set_visible(False)

divider = make_axes_locatable(axMain)
axLin = divider.append_axes("left", size=2.0, pad=0, sharey=axMain)
axLin.set_xscale('log')
axLin.set_xlim((1, 25000))
axLin.plot(x[0], perf, linestyle=':', marker='o', color = "#958934", label="Perfect", linewidth = 3.5, markersize = 0)
axLin.plot(x[:5], y[:5], linestyle='-', marker='o',color = "#397624", label = "Forecasting", linewidth = 3.5, markersize = 15)
axLin.plot(x[0], base, linestyle=':', marker='o', color = "#9fc55a", label="No Forecasting", linewidth = 3.5, markersize = 0)
axLin.spines['right'].set_visible(False)
axLin.yaxis.set_ticks_position('left')
plt.setp(axLin.get_xticklabels(), visible=True)

axMain.legend(fancybox=False,
            edgecolor="#000",
            fontsize="medium",
            loc="lower right",
         #bbox_to_anchor=(0.41, 1)
         )

axMain.axhline(
        y=perf,
        linestyle=":",
        linewidth=3.5,
        color="#958934",
    )

axLin.axhline(
        y=perf,
        linestyle=":",
        linewidth=3.5,
        color="#958934",
    )

axMain.axhline(
        y=base,
        linestyle=":",
        linewidth=3.5,
        color="#9fc55a",
    )

axLin.axhline(
        y=base,
        linestyle=":",
        linewidth=3.5,
        color="#9fc55a",
    )

axMain.set_xlabel("Epoch length (insert ops)")
axLin.set_ylabel('Throughput (kreq/s)')

fig.tight_layout()
plt.savefig("epoch_length_comparison.pdf")

plt.show()



# In[ ]:




