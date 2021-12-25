import pyspark
import time
import re

WORD_REGEX = re.compile(r"\b\w+\b")

sc = pyspark.SparkContext()


def check_blocks(line):
    try:
        fields = line.split(',')
        if len(fields) != 9:
            return False
        int(fields[7])
        int(fields[6])
        return True
    except:
        pass


blocks = sc.textFile("/data/ethereum/blocks")
good_blocks = blocks.filter(check_blocks)
blocks_split = good_blocks.map(lambda x: x.split(','))
time_epoch_gas_used = blocks_split.map(lambda j: (int(j[7]), int(j[6]), 1))

yearly_gas_used = time_epoch_gas_used.map(lambda t: (time.strftime("%m/%Y", time.gmtime(t[0])), (t[1], t[2])))

yearly_agg_gas_used = yearly_gas_used.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
yearly_average_gas_used = yearly_agg_gas_used.map(lambda x: (x[0], (x[1][0] / x[1][1])))

yearly_average_gas_used.saveAsTextFile('Guzzler2')
