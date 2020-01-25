from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class Node:
	def __init__(self):
		self.characterID = ''
		self.connections = []
		self.distanse = 9999
		self.color = 'WHITE'


	def fromLine(self, line):
		fields = line.split('|')
		if (len(fields)==4):
			self.characterID = fields[0]
			self.connections = fields[1].split(',')
			self.distanse = int(fields[2])
			self.color = fields[3]



	def getLine(self):
		connections = ','.join(self.connections)
		return '|'.join((self.characterID, connections, str(self.distanse), self.color))



class MRBFSIteration(MRJob):

	# Input file must be created using ProcessMarvel.py

	INPUT_PROTOCOL = RawValueProtocol
	OUTPUT_PROTOCOL = RawValueProtocol

	def configure_options(self):
		super(MRBFSIteration, self).configure_options()
		self.add_passthrough_option(
			'--target', help="ID of character we are searching for")
		self.add_file_option('--names', help='Path ro Marvel-names.txt')



	def mapper_init(self):
		self.heroNames = {}
		with open("Marvel-Names.TXT", encoding='ascii', errors='ignore') as f:
			for line in f:
				fields = line.split('"')
				self.heroNames[int(fields[0])] = fields[1]



	def mapper(self,_,line):
		node = Node()
		node.fromLine(line)
		#explore node if it's gray
		if node.color == 'GRAY':
			for connection in node.connections: #go to connection
				vnode = Node() #create new node
				vnode.characterID = connection
				vnode.distanse = int(node.distanse)+1
				vnode.color = 'GRAY'

				#increment counter if target was found
				if self.options.target == connection:
					counterName = ("Traget ID " + connection + " traget Name "+ self.heroNames[int(connection)] +
						" was hit with distanse " + str(vnode.distanse))
					self.increment_counter('Degrees of Separation', 
						counterName, 1)
				yield connection, vnode.getLine()

			#Node is processed - change color to black
			node.color = 'BLACK'
		#Emit the imput node
		yield node.characterID, node.getLine()





	def reducer (self, key, values):
		edges = []
		distanse = 9999
		color = 'WHITE'

		for value in values:
			node = Node()
			node.fromLine(value)

			if len(node.connections) > 0:
				edges.extend(node.connections)


			if node.distanse < distanse:
				distanse = node.distanse

			if node.color == 'BLACK':
				color = 'BLACK'

			if node.color == 'GRAY' and color == 'WHITE':
				color = 'GRAY'


		node = Node()
		node.characterID = key
		node.distanse = distanse
		node.color = color
		node.connections = edges


		yield key, node.getLine()



if __name__ == '__main__':
	MRBFSIteration.run()









