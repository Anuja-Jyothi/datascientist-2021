from mrjob.job import MRJob
import re
import time
import datetime
class tpm(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        timestmp = fields[7]
        txncount = fields[8]
        if (timestmp == "timestamp" or txncount == "0"):
            timestmp = ''
            txncount = ''
        else:
            monthyr=time.strftime('%m-%Y', time.localtime(int(timestmp)))
            yield (monthyr,int(txncount))

    def reducer(self, timestmp, txncounts):
        yield (timestmp, sum(txncounts))

if __name__ == '__main__':
    tpm.run()
