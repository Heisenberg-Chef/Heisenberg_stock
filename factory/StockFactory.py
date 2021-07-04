# -*- coding: UTF-8 -*-
import datetime
import time
import random

from threading import Lock

import baostock as bs

import pandas as pd
import tushare as ts

import os
from tqdm import tqdm

mutex = Lock()


class StockFactory:
    KDJ = 9

    def __init__(self):
        self.count = 0
        lg = bs.login()
        if os.path.exists("./StockList.csv"):
            self.stock_list = pd.read_csv("./StockList.csv")
        else:
            self.get_list()
            self.stock_list = pd.read_csv("./StockList.csv")
        if not os.path.exists("./stock_data"):
            os.mkdir("./stock_data")

    # 更新A股列表
    def get_list(self):
        """
        更新股票列表，如果已经有StockList.csv，那么就将其删除之后再进行下载操作。
        :return: None
        """
        if os.path.exists("./StockList.csv"):
            os.remove("./StockList.csv")
        token = "c6733d26d1e56dbb1521266041825e36215fd87c7d0635054db0ba1b"
        pro = ts.pro_api(token=token)
        data = pro.query('stock_basic', exchange='', list_status='L',
                         fields='ts_code,symbol,name,area,industry,'
                                'list_date,fullname,enname,market')
        lists = []
        exchange = []
        for i in data['ts_code']:
            arr = i.split('.')
            if arr[1] == 'SH':
                lists.append("sh" + "." + arr[0])
                exchange.append("沪")
            else:
                lists.append("sz" + "." + arr[0])
                exchange.append("深")
        data["code"] = lists
        data["exchange"] = exchange
        data.drop("ts_code", axis=1, inplace=True)
        with open('./StockList.csv', 'w') as f:
            data.to_csv(f)

    # abort
    def _bs_get_list(self):
        rs = bs.query_all_stock("2021-07-02")
        stock_list = rs.get_data()
        with open('./StockList2.csv', 'w') as f:
            stock_list.to_csv(f)

    # 创建并覆盖旧数据，初始化使用为佳
    def get_data(self, code="", name="", delta_days=0):
        if code and name == "":
            return None
        # 最近一天
        # end_val = datetime.date.today()
        end_val = datetime.date.today()
        if delta_days != 0:
            start_val = (end_val - datetime.timedelta(days=delta_days))
        else:
            start_val = (end_val - datetime.timedelta(days=365 * 10))
        end_val = end_val.isoformat()

        start_val = start_val.isoformat()
        ################################################
        #   baostock - 下载股票数据
        ################################################
        # lg = bs.login()
        # adjustflag : 默认为3，
        rs = bs.query_history_k_data_plus(code,
                                          "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                          start_date=start_val, end_date=end_val,
                                          frequency="d", adjustflag="2")
        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        stock_data = pd.DataFrame(data_list, columns=rs.fields)
        # 计算
        stock_data = self.calc(stock_data)
        stock_data.to_csv("./stock_data/" + code + "+" + name + ".csv")
        sleepTime = random.randint(50, 100) / 100
        time.sleep(sleepTime)

    # 更新数据到最近版本
    def update_data(self, code="", name=""):
        try:
            stock_data = pd.read_csv("./stock_data/" + code + "+" + name + ".csv")
        except:
            self.get_data(code, name)
            return 1
        if stock_data.empty:
            self.get_data(code, name)
            return 1
        start = stock_data["date"].tolist()[-1]
        end = datetime.date.today()
        week = datetime.date.weekday(end) # 获得今天是星期几
        if week == 5: # 周六 往前推一天
            end = end - datetime.timedelta(days=1)
        elif week == 6:
            end = end - datetime.timedelta(days=2)
        end = end.isoformat()
        if start == end:
            return 0
        rs = bs.query_history_k_data_plus(code,
                                          "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,"
                                          "tradestatus,pctChg,isST",
                                          start_date=start, end_date=end,
                                          frequency="d", adjustflag="2")
        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        stock_data_plus = pd.DataFrame(data_list, columns=rs.fields)
        # 表格合并
        stock_data = pd.concat([stock_data, stock_data_plus], ignore_index=True)
        # 删除包含Unnamed开头的列
        stock_data = stock_data.loc[:, ~stock_data.columns.str.contains("^Unnamed")]
        stock_date = self.calc(stock_data)
        stock_data.to_csv("./stock_data/" + code + "+" + name + ".csv")
        sleepTime = random.randint(10, 50) / 100
        time.sleep(sleepTime)
        return 1

    # 计算指标
    def calc(self, stock_data):

        ################################################################################
        # 将数据转化成float类型
        ################################################################################
        stock_data["turn"] = [0 if x == "" else float(x) for x in stock_data["turn"]]
        stock_data["open"] = [0 if x == "" else float(x) for x in stock_data["open"]]
        stock_data["close"] = [0 if x == "" else float(x) for x in stock_data["close"]]
        stock_data["high"] = [0 if x == "" else float(x) for x in stock_data["high"]]
        stock_data["low"] = [0 if x == "" else float(x) for x in stock_data["low"]]
        stock_data["preclose"] = [0 if x == "" else float(x) for x in stock_data["preclose"]]
        # 填充Vol的空缺值
        stock_data.fillna(stock_data['volume'], inplace=True)
        ########################################
        #   KDJ
        ########################################
        low_list = stock_data["low"].rolling(self.KDJ, min_periods=1).min()  # minimum price of period 9 days
        high_list = stock_data["high"].rolling(self.KDJ, min_periods=1).max()  # maximum price of period 9 days
        # RSV parameter : score between 1 - 100
        rsv = (stock_data['close'] - low_list) / (high_list - low_list) * 100
        stock_data['rsv'] = rsv
        stock_data['k'] = rsv.ewm(com=2, adjust=False).mean()
        stock_data['d'] = stock_data['k'].ewm(com=2, adjust=False).mean()
        stock_data['j'] = stock_data['k'] * 3 - 2 * stock_data['d']
        ########################################
        # building MACD data
        ########################################
        stock_data['exp12'] = stock_data['close'].ewm(span=12, adjust=False).mean()
        stock_data['exp26'] = stock_data['close'].ewm(span=26, adjust=False).mean()
        stock_data['macd_dif'] = stock_data['exp12'] - stock_data['exp26']
        stock_data['macd_dea'] = stock_data['macd_dif'].ewm(span=9, adjust=False).mean()
        stock_data['macd_bar'] = 2 * (stock_data['macd_dif'] - stock_data['macd_dea'])
        #########################################
        # MA
        #########################################
        stock_data['ma5'] = stock_data['close'].rolling(window=5).mean()
        stock_data['ma10'] = stock_data['close'].rolling(window=10).mean()
        stock_data['ma20'] = stock_data['close'].rolling(window=20).mean()
        stock_data['ma30'] = stock_data['close'].rolling(window=30).mean()
        stock_data['ma100'] = stock_data['close'].rolling(window=100).mean()
        #########################################
        # BOLL
        #########################################
        stock_data['up'] = stock_data['ma20'] + stock_data['close'].rolling(window=20).std() * 2
        stock_data['dn'] = stock_data['ma20'] - stock_data['close'].rolling(window=20).std() * 2
        return stock_data

    def create_data(self):
        query = {}
        for index, row in self.stock_list.iterrows():
            query[row["code"]] = row["name"]
        count = 0
        for code, name in tqdm(query.items()):
            # flag 位置是为了确定这1000次是否休息1分钟用的。
            # 如果0表示不用休息
            # 如果是1表示刚刚更新过股票，需要休息。
            try:
                flag = self.update_data(code, name)
            except:
                print(code,name)
                print("  - 该股票数据错误。")
                flag = 1
                exit(1)
            count += 1
            if count % 1000 == 0 and flag == 1:
                time.sleep(60)
        bs.logout()


if __name__ == "__main__":
    sf = StockFactory()
    sf.update_data("sh.603031", "测试")
