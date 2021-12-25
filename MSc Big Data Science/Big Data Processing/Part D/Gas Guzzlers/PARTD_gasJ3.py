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
        int(fields[3])
        return True
    except:
        pass


blocks = sc.textFile("/data/ethereum/blocks")
good_blocks = blocks.filter(check_blocks)
blocks_split = good_blocks.map(lambda x: x.split(','))
time_epoch_difficulty = blocks_split.map(lambda j: (int(j[7]), int(j[3]), 1))

yearly_difficulty = time_epoch_difficulty.map(lambda t: (time.strftime("%m/%Y", time.gmtime(t[0])), (t[1], t[2])))

yearly_agg_difficulty = yearly_difficulty.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
yearly_average_difficulty = yearly_agg_difficulty.map(lambda x: (x[0], (x[1][0] / x[1][1])))

yearly_average_difficulty.saveAsTextFile('Guzzler3')
