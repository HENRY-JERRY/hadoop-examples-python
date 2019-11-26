from abc import ABC

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRRatingCounter(MRJob, ABC):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_amount,
                   reducer=self.reducer_amount),
            MRStep(mapper=self.mapper_sort,
                   reducer=self.reducer_sort)
        ]

    def mapper_amount(self, _, line):
        (customer, item, amount) = line.split(',')
        yield customer, float(amount)

    def reducer_amount(self, customer, amt):
        yield customer, sum(amt)

    def mapper_sort(self, amt, customer):
        yield '%04.02f'%float(amt), customer

    def reducer_sort(self, orderTotal, customerID):
        for cid in customerID:
            yield cid, orderTotal


if __name__ == '__main__':
    MRRatingCounter.run()
