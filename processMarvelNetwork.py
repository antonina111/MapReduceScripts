import sys


print('Creating BFS starting input file for character ' + sys.argv[1])


with open ('BFS-iteration-0.txt', 'w') as out:
	with open ('Marvel-Graph.txt') as f:
		for line in f:
			fields = line.split()
			heroID = fields[0]
			numOfFrineds = len(fields) - 1
			connections = fields[1:]

			color = 'WHITE'
			distanse = 9999

			if sys.argv[1] == heroID:
				color= 'GRAY'
				distanse = 0

			if heroID != '':
				edges = ','.join(connections)
				outStr = '|'.join((heroID, edges, str(distanse), color))
				out.write(outStr)
				out.write("\n")

	f.close()
out.close()

