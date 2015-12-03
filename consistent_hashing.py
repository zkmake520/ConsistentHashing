import bisect
import hashlib

class ConsistentHashing:
	'''ConsistentHashing class creates a object of size n and using r replicas.
	 three attributes:numMachines,numReplicas,and machineDistribution a list of tuple (machine number, replica number, hashvalue)
	 of machine distributions on a ring
	 Method:getMachineNumber(key) returns the number of machine to which key should be mapped '''

	def __init__(self,numMachines =1,numReplicas=1):
		self.numMachines = numMachines
		self.numReplicas = numReplicas
		self.machineDistribution = [(i,j,getHashVal(str(i)+" "+str(j))) 
									for i in range(self.numMachines)
									for j in range(self.numReplicas)]

		self.machineDistribution.sort(lambda x,y:cmp(x[2],y[2]))

	def getMachineNumber(self,key):
		''' Returns the number of machine that stores this key'''
		hashValue = getHashVal(key)
		''' edge case '''
		if hashValue > self.machineDistribution[-1][2]:
			return self.machineDistribution[0]
		else:
			allHashValues = map(lambda x: x[2], self.machineDistribution)
			idx = bisect.bisect_left(allHashValues,hashValue)	
			return self.machineDistribution[idx]

def getHashVal(key):
		return (int(hashlib.md5(key).hexdigest(),16) % 1000000) / 1000000

def main():
	hashTable = ConsistentHashing(10,5)
	print "Format:"
  	print "(machine,replica,hashValue):"
  	for (j,k,h) in hashTable.machineDistribution: 
  		print "(%s,%s,%s)" % (j,k,h)
  	while True:
    	print "\nPlease enter a key:"
    	key = raw_input()
    	print "\nKey %s maps to hash %s, and so to machine %s" \
        	% (key,getHashVal(key),hashTable.getMachineNumber(key))

if __name__ == "__main__": 
	main()
