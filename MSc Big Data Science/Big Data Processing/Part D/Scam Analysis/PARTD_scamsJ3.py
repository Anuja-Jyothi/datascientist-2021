from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import time

class PartDScamsJob3(MRJob):
    def mapper_1(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                to_address = fields[2]
                yield to_address, (1,0)
            else:
                line = json.loads(lines)
                keys = line["result"]

                for key in keys:
                    record = line["result"][key]
                    category = record["category"]
                    addresses = record["addresses"]
                    status = record["status"]
                    for addr in addresses:
                        yield addr, (2, category,status)
        except:
            pass

    def reducer_1(self, key, values):
        total_value = 0
        category = None
        status = None
        for v in values:
            if v[0] == 1:
                total_value+=v[0]
            else:
                category = v[1]
                status = v[2]
        if category is not None and status is not None:
            yield (status, category), total_value

    def mapper_2(self,key,value):
        yield key,value

    def reducer_2(self,key,value):
        yield key,sum(value)

    def steps(self):
          return [MRStep(mapper=self.mapper_1,
                          reducer=self.reducer_1),
                          MRStep(mapper=self.mapper_2,
                                          reducer=self.reducer_2)]

if __name__ == '__main__':
    PartDScamsJob3.run()
