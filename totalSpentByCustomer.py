from mrjob.job import MRJob
from mrjob.step import MRStep


class totalSpentByCustomer(MRJob):

	def steps(self):
		return [MRStep(mapper=self.mapper_customer_amount, 
			combiner=self.combine_data,
			reducer = self.reducer_sum_amount_count),
		MRStep(reducer=self.reduce_sort)
		]


	def mapper_customer_amount(self, _, line):
		(customer, item, amount) = line.split(',')
		yield customer, float(amount)


	def combine_data(self, customer,amount):
		yield customer, sum(amount)


	def reducer_sum_amount_count(self, customer, amount):
		yield None, (sum(amount), customer)

	def reduce_sort(self, _, amount_counts):
		for amount, customer in sorted(amount_counts, reverse = True
			):
			yield (customer, '{:05.2f}'.format( float(amount) ) )



if __name__ == '__main__':
	totalSpentByCustomer.run()