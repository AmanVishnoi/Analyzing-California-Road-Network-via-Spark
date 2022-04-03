
import pandas as pd

data = pd.read_csv("results2.txt", header=None,names=["Common"])

data

clean_data = data["Common"].str.split(" ",expand=True)

clean_data

clean_data.rename(columns={0: 'time', 1: 'Averagedist'},inplace=True)  # old method  
clean_data

clean_data["time"] = pd.to_datetime(clean_data["time"])
clean_data["Averagedist"] = pd.to_numeric(clean_data["Averagedist"])

clean_data

clean_data = clean_data[clean_data["Averagedist"] != 0]
clean_data

import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
model = LinearRegression()
model.fit(clean_data["Averagedist"].to_frame(), clean_data["time"].to_frame())
ax = plt.axes()
ax.scatter(clean_data["Averagedist"], clean_data["time"])
ax.set_xlabel('x')
ax.set_ylabel('y')
ax.axis('tight')
plt.show()

