# -*- coding: UTF-8 -*-
import pandas as pd
from multiprocessing import Pool, managers

from tqdm import tqdm


class Analysis:
    def __init__(self):
        self.list = pd.read_csv("./StockList.csv")

    def processing(self, ):
        task = {}
        for index, row in self.list.iterrows():
            task[row["code"]] = row["name"]
        p = Pool(16)
        res = p.map(self.synthesis, tqdm(task.items()))
        kdj20 = []
        kdj50 = []
        count = 0
        for i in res:
            if i != None:
                for code, j in i.items():
                    for signal, rs in j.items():
                        if rs:
                            count += 1
                            print("#{} 股票代码：{}，股票名称：{}，符合{}买入点。 - 所属板块：{}。"
                                  .format(count,code, self.list[self.list["code"] == code]["name"].values[0], signal,
                                          self.list[self.list["code"] == code]["industry"].values[0]))
                            if signal == "kdj20":
                                kdj20.append(code)
                            if signal == "kdj50":
                                kdj50.append(code)
        title = "序号,股票代码,股票名称,所属板块,上市日期,买入点算法名称"
        text = title + "\n"
        count = 0
        for i in kdj20:
            count += 1
            text = text + "{},{},{},{},{},kdj20\n".format(count,
                                                          i,
                                                          self.list[self.list["code"] == i]["name"].values[0],
                                                          self.list[self.list["code"] == i]["industry"].values[0],
                                                          self.list[self.list["code"] == i]["list_date"].values[0],
                                                          )

        for i in kdj50:
            count += 1
            text = text + "{},{},{},{},{},kdj50\n".format(count,
                                                          i,
                                                          self.list[self.list["code"] == i]["name"].values[0],
                                                          self.list[self.list["code"] == i]["industry"].values[0],
                                                          self.list[self.list["code"] == i]["list_date"].values[0],
                                                          )

        with open("./res.csv","w") as f:
            f.write(text)
        print("*"*50)

    # 综合计算函数，用来一次读取多次计算使用
    def synthesis(self, task):
        data = self.loader(task)
        if data.empty:
            return None
        res = {task[0]: {}}
        kdj20 = self.kdj20(data)
        kdj50 = self.kdj50(data)
        res[task[0]]["kdj20"] = kdj20
        res[task[0]]["kdj50"] = kdj50
        return res

    # 加载数据函数
    def loader(self, task):
        try:
            data = pd.read_csv("./stock_data/" + task[0] + "+" + task[1] + ".csv")
        except:
            return pd.DataFrame()
        return data

    #################################################################
    # 相关算法
    # 所有算法都是二分类算法，目标就是告诉你是或者不是
    #################################################################
    def kdj20(self, data):
        if data["ma100"].values[-1] <= data["close"].values[-1]:
            # 说明确定上升趋势
            if data["k"].values[-2] < data["k"].values[-1] and data["k"].values[-1] > 20 and data["k"].values[-2] < 20:
                # 是否K穿20
                return True
            else:
                return False
        else:
            return False

    def kdj50(self, data):
        if data["ma100"].values[-1] <= data["close"].values[-1]:
            # 说明确定上升趋势
            if data["k"].values[-2] < data["k"].values[-1] and data["k"].values[-1] > 50 and data["k"].values[-2] < 50:
                # 是否K穿50
                days = 1  # 1代表今天
                # 接下来我们去找金叉死叉，确保上一个50之前只有一个金叉和死叉，否侧说明趋势中存在背离。
                flag = 0
                while True:
                    days += 1
                    if data["k"].values[-days] > 50:
                        break
                while True:
                    days += 1
                    if data["k"].values[-days] < 50:
                        break
                # 找金叉死叉
                gold = 0
                dead = 0
                for i in range(1, days):
                    if data["k"].values[-i] - data["d"].values[-i] < 0 and data["k"].values[-i - 1] - data["d"].values[
                        -i - 1] > 0:
                        dead += 1
                    if data["k"].values[-i] - data["d"].values[-i] > 0 and data["k"].values[-i - 1] - data["d"].values[
                        -i - 1] < 0:
                        gold += 1
                if gold == 1 and dead == 1:
                    return True
                else:
                    return False
            else:
                return False
        else:
            return False


if __name__ == "__main__":
    a = Analysis()
    a.processing()
