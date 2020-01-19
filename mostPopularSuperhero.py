from mrjob.job import MRJob
from mrjob.step import MRStep


class mostPopularSuperhero(MRJob):

	def configure_options(self):
		super(mostPopularSuperhero, self).configure_options()
		self.add_file_option('--names', help='Path ro Marvel-names.txt')

	def steps(self):
		return [
		MRStep(mapper = self.mapper_map_friends,
			reducer = self.reducer_count_friends),
		MRStep(mapper=self.mapper_prepare_for_sort,
			reducer_init=self.reducer_init_load_sp_names,
			reducer=self.reducer_get_most_popular_superhero
			)

		] 

	def mapper_map_friends(self, _, line):
		fields = line.split()
		yield int(fields[0]), int(len(fields)-1)


	def reducer_count_friends(self, spID, numOfFriends):
		yield spID, sum(numOfFriends)



	def mapper_prepare_for_sort(self, spID, numOfFriends):
		yield None, (numOfFriends, spID)


	def reducer_init_load_sp_names(self):
		self.heroNames = {}
		with open("Marvel-Names.TXT", encoding='ascii', errors='ignore') as f:
			for line in f:
				fields = line.split('"')
				self.heroNames[int(fields[0])] = fields[1]



	def reducer_get_most_popular_superhero(self, _, numOfFriends):
		most_popular_superhero = max(numOfFriends)
		yield self.heroNames[most_popular_superhero[1]], most_popular_superhero[0]
		#yield most_popular_superhero[1], most_popular_superhero[0]





if __name__ == "__main__":
	mostPopularSuperhero.run()

