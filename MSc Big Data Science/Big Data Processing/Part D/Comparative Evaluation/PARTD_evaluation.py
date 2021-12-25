import pyspark
from operator import add
from time import *

def is_good_transaction_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        int(fields[3])
        return True
    except:
        return False

def is_good_contract_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
    except:
        return False
    return True

def remove_none(features):
    try:
        if features[1][1] is None:
            return False
    except:
        return False
    return True

begin_time = time()
sc = pyspark.SparkContext()
transactions = sc.textFile("/data/ethereum/transactions/")
contracts = sc.textFile("/data/ethereum/contracts/")
transaction_features = transactions.filter(is_good_transaction_line).map(lambda l: (l.split(",")[2], int(l.split(",")[3])))
transaction_values = transaction_features.reduceByKey(add)
contract_address = contracts.filter(is_good_contract_line).map(lambda l: (l.split(",")[0], "contract"))
join_features = contract_address.leftOuterJoin(transaction_values)
filter_features = join_features.filter(remove_none)
top10 = filter_features.takeOrdered(10, key=lambda x: -x[1][1])
with open('evaluateD1.txt', 'w') as f:
    for record in top10:
        f.write("{} : {}\n".format(record[0], record[1][1]))
