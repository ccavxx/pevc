import pandas as pd
import time
import numpy as np
import re


def log(func):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"{now}: start {func.__name__}")
        f = func(*args, **kwargs)
        t2 = time.time()
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"{now}: completed {func.__name__} in {round(t2-t1, 0)} seconds")
        return f
    return wrapper

class Get_tables():

    def __init__(self, events_tb_path):
        self.events_tb_path = events_tb_path

    @log
    def get_event_investor_tb(self):
        """
        returns a table where each row is a event_id * investor_id pair
        including unknown investors
        """
        events_tb = pd.read_csv(self.events_tb_path)
        event_investor_tb = []
        for index, event in events_tb.iterrows():
            event_id = event.event_id
            investor_cols = event[['investor_ids', 'investor_leader_id', 'investor_names', 'investor_urls']]
            for ID, leader_id, name, url in zip(*[str(item).split(",") for item in investor_cols]):
                if ID == leader_id :
                    role = "leader" if event.investor_number > 1 else "sole"
                else:
                    role = "follower"
                event_investor_tb.append([event_id, ID, name, url, role])
        event_investor_tb = pd.DataFrame(data=event_investor_tb,
                                         columns=['event_id', 'investor_id', 'investor_name', 'investor_url', 'role'])
        return event_investor_tb

    @log
    def get_investors_tb(self, drop_unknown=True):
        """
        returns a table containing the details of investors.
        each row is an investor.
        ordered by descending number of events involved.
        to be used as the nodes table for subsequent network data analysis.

        self.events_tb_path: path of the events csv table
        """
        event_investor_tb = self.get_event_investor_tb()
        investors_tb = event_investor_tb.drop(['event_id', 'role'], axis=1)
        investors_tb = investors_tb.groupby(investors_tb.columns.to_list()).size().reset_index(name='occurrences')
        investors_tb = investors_tb.sort_values('occurrences', ascending=False)
        if drop_unknown:
            investors_tb = investors_tb[investors_tb['investor_id']!='9999999']
            print("dropped unknown investor")
        return investors_tb

    @log
    def get_investors_tb_with_unique_id(self, investors_tb_path):
        """
        since an investor may have multiple ids in the system,
        for convenience purporse
        we want to generate a table that each row is a different investor,
        with columns: investor_name, innvestor_id_main, investor_id_other, total_occurrences, url_main
        """
        investors = pd.read_csv('data/investors.csv')
        investors_cp = investors.copy()
        occurrences_rank = investors_cp.groupby('investor_name')['occurrences'].rank(ascending=False, method='first')
        investors_cp['occurrences_rank'] = occurrences_rank.values
        investors_cp = investors_cp.drop(['investor_url', 'occurrences'], axis=1)
        name_main_other = investors_cp.pivot_table(index=['investor_name'],
                                                    columns='occurrences_rank',
                                                    values='investor_id').reset_index()
        name_main_other.columns = ['investor_name', 'investor_id_main',
                                   *[f'investor_id_other_{i}' for i in range(1, len(name_main_other.columns)-1)]]

        name_ttlocc = investors.groupby('investor_name')['occurrences'].sum().reset_index()
        name_ttlocc.columns = name_ttlocc.columns.str.replace("occurrences", "total_occurrences")
        investors_unique = name_main_other.merge(name_ttlocc, on='investor_name', how='left')
        investors_unique.investor_id_main = investors_unique.investor_id_main.astype(int)
        investors_unique['url_main'] = "https://data.cyzone.cn/capital/" \
                                        + investors_unique.investor_id_main.astype(str) \
                                        + ".html"
        investors_unique = investors_unique.sort_values('total_occurrences', ascending=False)
        return investors_unique

    @log
    def get_coinv_pairs(self, investors_unique_tb_path):
        """
        return a table about the co-investment relation of investors.
        row: a pair of investors in an event
        cols: event_id, investor1_id, investor2_id, investor1_name, investor2_name, relation
        to be used as the edges table for subsequent network data analysis.
        self.events_tb_path: path of the events csv table
        """
        events_tb = pd.read_csv(self.events_tb_path)
        investor_pairs = []
        for index, event in events_tb.iterrows():
            if event.investor_number == 0: # unknown
                pass
            elif event.investor_number == 1: # sole investor
                investor1_id = investor2_id = event.investor_ids
                investor1_name = investor2_name = event.investor_names
                relation = 'sole'
                investor_pairs.append([event.event_id,
                                       investor1_id, investor2_id,
                                       investor1_name, investor2_name,
                                       relation])
            else: # co-invest event
                investor_ids = event.investor_ids.split(",")
                investor_names = event.investor_names.split(",")
                for i in range(len(investor_ids)-1):
                    relation = 'leader_follower' if i==0 else 'follower_follower'
                    for j in range(i+1, len(investor_ids)):
                        investor_pairs.append([event.event_id,
                                               investor_ids[i], investor_ids[j],
                                               investor_names[i], investor_names[j],
                                               relation])

        investor_pairs = pd.DataFrame(data = investor_pairs,
                                      columns = ["event_id",
                                                 "investor1_id", "investor2_id",
                                                 "investor1_name", "investor2_name",
                                                 "relation"])
        # replace dup id
        investors_unique = pd.read_csv(investors_unique_tb_path)
        investor_pairs = investor_pairs.merge(investors_unique[["investor_name", "investor_id_main"]],
                                              left_on="investor1_name", right_on="investor_name")
        investor_pairs.columns = investor_pairs.columns.str.replace("investor_id_main", "investor1_id_main")
        investor_pairs = investor_pairs.drop('investor_name', axis=1)
        investor_pairs = investor_pairs.merge(investors_unique[["investor_name", "investor_id_main"]],
                                              left_on="investor2_name", right_on="investor_name")
        investor_pairs.columns = investor_pairs.columns.str.replace("investor_id_main", "investor2_id_main")
        investor_pairs = investor_pairs.drop('investor_name', axis=1)
        investor_pairs = investor_pairs.drop(['investor1_id', 'investor2_id'], axis=1)
        return investor_pairs

    def get_degree_tb(self, investor_pairs_tb_path):
        investor_pairs = pd.read_csv(investor_pairs_tb_path)
        nodes = set(investor_pairs.Leader).union(investor_pairs.Follower)
        degrees = []
        for node in nodes:
            in_degree = sum(investor_pairs.Leader==node)
            out_degree = sum(investor_pairs.Follower==node)
            degree = in_degree + out_degree
            degrees.append([degree, in_degree, out_degree])
        degree_tb = pd.DataFrame(degrees, columns=['degree', 'in_degree', 'out_degree'])
        degree_tb['node'] = nodes
        degree_tb.insert(0, 'node', degree_tb.pop('node'))
        degree_tb.sort_values('degree', ascending=False, inplace=True)
        degree_tb.index = range(len(degree_tb))
        return degree_tb



    def get_gephi_nodes_edges_tbs(self, investors_unique_tb_path, investor_pairs_tb_path):
        investors_unique = pd.read_csv(investors_unique_tb_path)
        investors_gephi = investors_unique[investors_unique['total_occurrences']>20]
        investors_gephi = investors_gephi.drop(['investor_id_other_1', 'investor_id_other_2'], axis=1)
        investors_gephi.rename(columns={'investor_id_main':'Id', 'url_main':'URL',
                                        'investor_name':'Name_Chinese', 'investor_name_eng':'Name_English',
                                        'total_occurrences':'Number of investment events'
                                        },inplace=True)

        investor_pairs_by_event = pd.read_csv(investor_pairs_tb_path)
        investor_pairs_by_event.drop('event_id', axis=1, inplace=True)
        investor_pairs = investor_pairs_by_event.groupby(investor_pairs_by_event.columns.to_list()).size().reset_index(name='Weight')
        num_totol_edges = len(investor_pairs)
        investor_pairs = investor_pairs[investor_pairs.investor1_id_main.isin(investors_gephi.Id)]
        investor_pairs = investor_pairs[investor_pairs.investor2_id_main.isin(investors_gephi.Id)]
        investor_pairs = investor_pairs[investor_pairs.relation!="follower_follower"] # otherwise too many edges
        # delete isolated nodes
        coop_investors = investor_pairs.loc[investor_pairs.relation=='leader_follower', ['investor1_name', 'investor2_name']]
        sole_investors = investor_pairs.loc[investor_pairs.relation=='sole', ['investor1_name']]
        sole_only = set(sole_investors.iloc[:,0]) - (set(coop_investors.iloc[:,0]).union(set(coop_investors.iloc[:,1])))
        investor_pairs = investor_pairs[~investor_pairs.investor1_name.isin(sole_only)]
        investor_pairs = investor_pairs[~investor_pairs.investor2_name.isin(sole_only)]
        investors_gephi = investors_gephi[~investors_gephi.Name_Chinese.isin(sole_only)]
        print(f"Gephi will use {len(investors_gephi)} investors from {len(investors_unique)} as nodes")

        investor_pairs.drop('relation', axis=1, inplace=True)
        investor_pairs.rename(columns={'investor1_id_main':'Target', 'investor2_id_main':'Source',
                                        'investor1_name':'Leader', 'investor2_name':'Follower'
                                        },inplace=True)
        investor_pairs['Id'] = investor_pairs.Target.astype(str) + investor_pairs.Source.astype(str) # edge id
        print(f"Gephi will use {len(investor_pairs)} investor pairs from {num_totol_edges} as edges")

        return investors_gephi, investor_pairs




class Transformer():

    def __init__(self, amount_tb_path, dictionary_path):
        self.amount_tb_path = amount_tb_path
        self.dictionary_path = dictionary_path

    @log
    def chinese_to_english(self, df, colnames, types, drop_chn=False):
        """
        translate Chinese to English according to a dictionary
        df: a data frames contains the column to be translated
        colnames: a list of columns to be translated
        dictionary: colummns include chinese, english, type (seires, investor_name)
        """
        dictionary = pd.read_csv(self.dictionary_path)
        for colname, type in zip(colnames, types):
            dictionary = dictionary[dictionary['type']==type]
            df = df.merge(dictionary, left_on=colname, right_on='chinese', how='left')
            df.columns = df.columns.str.replace('english', colname+'_eng')
            df = df.drop(['chinese', 'type'], axis=1) # chinese and columns in the dictionary
            if drop_chn:
                df = df.drop(colname, axis=1) # chinese column in the original data frame
        return df

    def amount_to_cny(self, amount):
        """
        convert a text amount to a number in Chinese Yuan
        amounts:
        """

        # 1. unknown
        unknowns = ["未公开", "未公布", "未透露", "未披露", "不详", "暂不透露", "不方便透露", "未知"]
        if any(unknown in amount for unknown in unknowns):
            return np.nan

        # 2. standard format: prefix + number + suffix (unit+currency)
        prefices = {"":1, "超":1.1, "超过":1.1, "逾":1.1, "过":1.1, "上":1.1, \
                    "近":0.9, "约":0.9, "数":5}
        units = {"":1, '千':10**3, '万':10**4, "多万":1.2*10**4, '十万':10**5, \
                '百万':10**6, '百万级':10**6, '千万':10**7, '千万级':10**7, '亿':10**8, \
                '亿级':10**8, '亿元级':10**8, '十亿':10**9, '亿元及以上':1.2*10**8}
        currencies = {"元人民币":1, "人民币":1, "美元":7, "美金":7, "港元":0.9, "港币":0.9, \
                      "加元":1, "加拿大元":1, "澳元":1, "卢比":1, "新台币":1, "新加坡元":1, \
                      "英镑":1, "日元":1, "欧元":1, "以太坊":1, "元":1, "":1}
        suffices = {}
        for unit in units.keys():
            for currency in currencies.keys():
                key = unit+currency
                value = units[unit] * currencies[currency]
                suffices[key] = value

        # check if standard format
        finder1 = re.search("\d+\.\d+", amount)
        finder2 = re.search("\d+", amount)
        if finder1 or finder2:
            if finder1: # matched dd.dd
                num = finder1.group()
                prefix, suffix = amount.split(num)
            else: # mathed ddd
                num = finder2.group()
                prefix, suffix = amount.split(num)

            if prefix in prefices and suffix in suffices:
                return float(prefices[prefix]*float(num)*suffices[suffix])

        # 3. prefix + suffix
        ps = {}
        for prefix in prefices:
            for suffix in suffices:
                key = prefix+suffix
                if key: # not ""
                    value = prefices[prefix]*suffices[suffix]
                    ps[key] = value
        if amount in ps.keys():
            return float(ps[amount])

        # 4. unidentifiable
        unidentifiable_amount_tb = pd.read_csv(self.amount_tb_path)
        result = unidentifiable_amount_tb.loc[unidentifiable_amount_tb.amount==amount, 'result']
        return float(result)

    @log
    def amounts_to_cny(self, amounts):
        results = []
        for amount in amounts:
            results.append(self.amount_to_cny(amount))
        return results


    def amount_to_usd(self, amounts, usd_cny_rate=7):
        """
        convert text amount to numbers in USD
        amounts:
        """
        return self.amount_to_cny(amounts)/usd_cny_rate

    @log
    def amounts_to_usd(self, amounts, usd_cny_rate=7):
        results = []
        for amount in amounts:
            results.append(self.amount_to_usd(amount))
        return results

if __name__== '__main__':
    # pass
    events_tb_path = 'data/events.csv'
    events_tb = pd.read_csv('data/events.csv')

    get_tables = Get_tables(events_tb_path)

    event_investor_tb = get_tables.get_event_investor_tb()

    investors = get_tables.get_investors_tb()
    investors.to_csv(f'data/investors.csv', index=False)

    investors_unique = get_tables.get_investors_tb_with_unique_id('data/investors.csv')
    investors_unique.to_csv('data/investors_unique.csv', index=False)

    investor_pairs = get_tables.get_coinv_pairs('data/investors_unique.csv')
    investor_pairs.to_csv(f'data/investor_pairs.csv', index=False)
    investor_pairs.head()

    transformer = Transformer(amount_tb_path = 'data/unidentifiable_amount.csv',
                              dictionary_path = 'data/dictionary.csv')
    events_tb2 = events_tb.copy()
    events_tb2['amount_cny'] = transformer.amounts_to_cny(events_tb['amount'])
    events_tb2['amount_usd'] = transformer.amounts_to_usd(events_tb['amount'])
    investors_unique = transformer.chinese_to_english(investors_unique,
                                                      colnames=['investor_name'],
                                                      types=['investor_name'])
    investors_unique.to_csv('data/investors_unique.csv', index=False)
    investors_gephi, investor_pairs_gephi = get_tables.get_gephi_nodes_edges_tbs('data/investors_unique.csv',
                                                                                  'data/investor_pairs.csv')
    investors_gephi.to_csv('gephi/investors_gephi.csv', index=False)
    investor_pairs_gephi.to_csv('gephi/investor_pairs_gephi.csv', index=False)

    degree_tb = get_tables.get_degree_tb('gephi/investor_pairs.csv')
# some issues
## 0. investors.investor_id.value_counts()[:20]
## 1. manual input error of 76 dup events? sum(events.event_id.value_counts()==2)
## 2. nick name of investors, e.g. 红杉资本中国/红杉资本中国基金. fix this for top investors

# investors = pd.read_csv('data/investors.csv')
# ~10000 investors
# 3. get english name for top 100 investors
