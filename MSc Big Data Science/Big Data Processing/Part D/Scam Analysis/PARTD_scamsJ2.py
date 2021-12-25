from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import time

class ScamsJ2(MRJob):
    def mapper_1(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                to_address = fields[2]
                value = float(fields[3])
                time_epoch = int(fields[6])
                months = time.strftime("%m",time.gmtime(time_epoch))
                years = time.strftime("%y",time.gmtime(time_epoch))
                date_combined = str(months) + '-' + str(years)
                yield to_address, (0, date_combined, value)
            else:
                line = json.loads(lines)
                keys = line["result"]

                for key in keys:
                    record = line["result"][key]
                    category = record["category"]
                    addresses = record["addresses"]

                    for addr in addresses:
                        yield addr, (1, category)
        except:
            pass

    def reducer_1(self, key, values):
        total_value = 0
        category = None
        date_combined = None
        for v in values:
            if v[0] == 0:
                date_combined = v[1]
                total_value = v[2]
            else:
                category = v[1]
        if category is not None and date_combined is not None:
            yield (date_combined, category), total_value

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
    ScamsJ2.run()
