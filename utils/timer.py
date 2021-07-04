# -*- coding: UTF-8 -*-
from datetime import datetime
import time

def timer(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        res = func(*args, **kwargs)
        stop_time = datetime.now()
        print('*   Runtime is %s' % (stop_time - start_time))
        return res
    return wrapper


@timer
def test():
    sum = 1
    time.sleep(2)
    for i in range(1000):
        sum += 1


if __name__ == "__main__":
    test()
# for i in range(len(self.stock_list_pd)):
#     if i % 4 == 0:
#         task1.append((self.stock_list_pd.index[i]
#                       , self.stock_list_pd.iloc[i:"query_code"]
#                       , self.stock_list_pd.iloc[i:"name"]))
#     if i % 4 == 2:
#         task2.append((self.stock_list_pd.index[i]
#                       , self.stock_list_pd.iloc[i:"query_code"]
#                       , self.stock_list_pd.iloc[i:"name"]))
#     if i % 4 == 3:
#         task3.append((self.stock_list_pd.index[i]
#                       , self.stock_list_pd.iloc[i:"query_code"]
#                       , self.stock_list_pd.iloc[i:"name"]))
#     if i % 4 == 1:
#         task4.append((self.stock_list_pd.index[i]
#                       , self.stock_list_pd.iloc[i:"query_code"]
#                       , self.stock_list_pd.iloc[i:"name"]))