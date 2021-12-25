
import pyspark
from operator import add
from time import *

def is_good_blocks_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 9:
            return False
        int(fields[6])
        return True
    except:
        return False

def is_good_transactions_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        return True
    except:
        return False

def is_good_topten_line(line):
    try:
        fields = line.split("\t")
        if len(fields) != 2:
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

def is_top10(features):
    try:
        if features[1][0] is None:
            return False
    except:
        return False
    return True

sc = pyspark.SparkContext()
blocks = sc.textFile("/data/ethereum/blocks/")
transactions = sc.textFile("/data/ethereum/transactions/")
partboutput = sc.textFile("/user/ajv31/partBtoD.txt")
blocks_features = blocks.filter(is_good_blocks_line).map(lambda l: (l.split(",")[0], int(l.split(",")[6])))
blocks_values = blocks_features.reduceByKey(add)
transactions_features = transactions.filter(is_good_transactions_line).map(lambda l: (l.split(",")[0], l.split(",")[2]))
transaction_values = blocks_values.join(transactions_features)
new_transaction_values = transaction_values.map(lambda l: (l[1][1], l[1][0])).reduceByKey(add)
topten_address = partboutput.filter(is_good_topten_line).map(lambda l: (l.split("\t")[0], "topten"))
join_features = new_transaction_values.leftOuterJoin(topten_address)
filter_features = join_features.filter(remove_none)
top10_features = filter_features.filter(is_top10)
final_output = top10_features.map(lambda l: (l[0], l[1][0]))
final_output.saveAsTextFile('Guzzler4')
