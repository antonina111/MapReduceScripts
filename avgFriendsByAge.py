from mrjob.job import MRJob
from numpy import mean

class avgNumOfFR(MRJob):
	def mapper(self,key,line):
		(userID, name, age, numOfFR) = line.split(',')
		yield age, float(numOfFR)

	def reducer(self, age, numOfFR):
		total=0
		numOfElements=0

		for x in numOfFR:
			total+=x
			numOfElements+=1

		yield age, total/numOfElements


if __name__ == '__main__':
	avgNumOfFR.run()