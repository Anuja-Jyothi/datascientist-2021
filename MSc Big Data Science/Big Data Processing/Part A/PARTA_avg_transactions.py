from mrjob.job import MRJob
import re
import time
import datetime

class avgtxn(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        tmpstmp = fields[6]
        value = fields[3]
        if (tmpstmp == "block_timestamp"):
            tmpstmp = ''
            value = ''
        else:
            monyr=time.strftime('%m-%Y', time.localtime(int(tmpstmp)))
            yield (monyr,(1,int(value)))

    def combiner(self, monyr, values):
        val_amt = 0
        txn_cnt = 0
        for value in values:
            txn_cnt += value[0]
            val_amt += value[1]
        yield (monyr, (txn_cnt, val_amt) )

    def reducer(self, monyr, values):
        val_amt = 0
        txn_cnt = 0
        for value in values:
            txn_cnt += value[0]
            val_amt += value[1]
        yield (monyr, val_amt/txn_cnt)

if __name__ == '__main__':
    avgtxn.run()