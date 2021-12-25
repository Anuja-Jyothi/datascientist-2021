from mrjob.job import MRJob
from mrjob.step import MRStep


class PartB(MRJob):
    def mapper_1(self, _, line):
        try:
            if len(line.split(",")) == 7:
                fields = line.split(",")
                to_address = fields[2]
                value = int(fields[3])
                yield to_address, (1, value)
            elif len(line.split(",")) == 5:
                fields = line.split(",")
                address = fields[0]
                yield address, (2, 1)
        except:
            pass

    def reducer_1(self, key, values):
        check = False
        all_values = []

        for value in values:
            if value[0] == 1:
                all_values.append(value[1])
            elif value[0] == 2:
                check = True

        if check:
            yield key, sum(all_values)

    def mapper_2(self, key, value):
        yield None, (key, value)

    def reducer_2(self, _, key_pair):
        sorted_values = sorted(key_pair, reverse=True, key=lambda x: x[1])

        for value in sorted_values[:10]:
            yield value[0], value[1]

    def steps(self):
        return [MRStep(mapper=self.mapper_1,
                       reducer=self.reducer_1),
                MRStep(mapper=self.mapper_2,
                       reducer=self.reducer_2)]


if __name__ == '__main__':
    PartB.run()
