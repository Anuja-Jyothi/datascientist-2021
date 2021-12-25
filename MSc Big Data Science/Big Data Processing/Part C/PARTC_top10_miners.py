from mrjob.job import MRJob
from mrjob.step import MRStep

class topM(MRJob):
    def mapper_1(self, _, line):
        data = line.split(',')
        try:
            if len(data)==9:
                miner = data[2]
                sizeBlock = data[4]
                yield miner, int(sizeBlock)
        except:
            pass

    def reducer_1(self, miner, size):
        try:
            yield miner, sum(size)
        except:
            pass

    def mapper_2(self, miner, size_total):
        try:
            yield None,(miner,size_total)
        except:
            pass

    def reducer_2(self, _, miner_size):
        try:
            sorted_size = sorted(miner_size, reverse=True, key=lambda x: x[1])
            for value in sorted_size[:10]:
                yield value[0], value[1]
        except:
            pass


    def steps(self):
          return [MRStep(mapper=self.mapper_1,
                          reducer=self.reducer_1),
                          MRStep(mapper=self.mapper_2,
                                          reducer=self.reducer_2)]

if __name__ == '__main__':
    topM.run()
