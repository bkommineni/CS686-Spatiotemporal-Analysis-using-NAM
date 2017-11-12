import matplotlib.pyplot as plt
import numpy as np

y_axis = np.array([13.282794,14.652242,12.744508,12.701427,14.701295,17.39771,22.180243,17.302666,16.938585,18.55326,12.198977,15.02631])

x_labels = np.array(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],
      dtype='|S13')

w = 3
nitems = len(y_axis)
x_axis = np.arange(0, nitems*w, w)

fig, ax = plt.subplots(1)
ax.bar(x_axis, y_axis, width=w, align='center',color='green')
ax.set_xticks(x_axis);
ax.set_xticklabels(x_labels, rotation=90);
plt.show()
