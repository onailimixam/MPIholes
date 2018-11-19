from mpi4py import MPI
import xml.etree.ElementTree as ET
from math import sin, cos, sqrt, atan2, radians
from sys import argv
import socket
import sys

def distanceCoordinates(lng1, lt1, lng2, lt2):
    # approximate radius of earth in km
    R = 6373.0

    lat1 = radians(lt1)
    lon1 = radians(lng1)
    lat2 = radians(lt2)
    lon2 = radians(lng2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

mainProcess = false
R = int(argv[1])
S = int(argv[2])


if (rank == 0):
	

	#sacar todos los puntos

	maxminTrip = [[0 for x in range(3)] for y in range(520)]

	#Create matrix [trips, minVal, maxVal] with enumerated trips
	for i in range(1,520):
	    tempArray = maxminTrip[i-1]
	    tempArray[0] = i

	print('Zeros matrix created')

	contP = 0
	maxTripID = 0

	for child in root:
	    for i, row in enumerate(child):
	        if i != 0:
	            currentTrip = row[1].text
	            tempMatrix = maxminTrip[int(currentTrip)-1]
	            minMatrix = tempMatrix[1]
	            maxMatrix = tempMatrix[2]

	            currentAce = row[7].text
	            if (float(currentAce) < float(minMatrix)):
	                tempMatrix[1] = currentAce
	            if (float(currentAce) > float(maxMatrix)):
	                tempMatrix[2] = currentAce

	            if (int(currentTrip) > int(maxTripID)):
	                maxTripID = int(currentTrip)

	            maxminTrip[int(currentTrip)-1] = tempMatrix

	            contP = contP + 1

	print(contP,' points found')    #162649 points
	print('Trips, maximum and minimum values stored')

	normMatrix = []
	greatS = []
	contS = 0

	for child in root:
	    for i, row in enumerate(child):
	        if i != 0:
	            currentTrip = row[1].text
	            tempMatrix = maxminTrip[int(currentTrip)-1]
	            minMatrix = tempMatrix[1]
	            maxMatrix = tempMatrix[2]
	            currentAce = row[7].text

	            normVal = (float(currentAce)-float(minMatrix))/(float(maxMatrix)-float(minMatrix))
	            
	            currentLat = float(row[3].text)
	            currentLng = float(row[4].text)

	            if normVal >= S:
	                greatS.append([normVal, currentLat, currentLng])
	                contS = contS + 1
	            else:
	                normMatrix.append([normVal, currentLat, currentLng])
	                

	print('Norms, longitude and latitude matrix created')
	print(contS,' points greater that ',S,' found')  #13566 points greater that S

	

	print('Processing center and neighbor points...')

	finalVals = []
	contProS = 0
	contProN = 0
	sendingCore = 1

	mainProcess = true
	comm.bcast(mainProcess, root=0)

	#mando la matriz completa de puntos
	#for i in range(1,size): comm.send(normMatrix, dest=i)

	#comienzo a mandar puntos
	for pS in greatS:
	    aceC = pS[0]
	    longC = pS[1]
	    latC = pS[2]
	    
	    pointsAvg = []
	    pointsAvg.append(aceC)
	    
	    for pN in normMatrix:
	        aceN = pN[0]
	        longN = pN[1]
	        latN = pN[2]
	        distance = distanceCoordinates(longC, latC, longN, latN)
	        if distance <= 30:
	            pointsAvg.append(aceN)

		#para cada punto lo saco de la cola y lo mando con send
        comm.send(pointsAvg, dest=sendingCore)
        sendingCore = (sendingCore + 1) % size
		if sendingCore == 0: sendingCore = 1;
	    #greatS.remove(pS)
	    contProS = contProS + 1

	for i in range(1,size): comm.send(False, dest=i)
	for i in range(1,size):
		res = comm.recv(source=i)
		for x in range(0,len(res)): finalVals.append(res[x])
	print(finalVals);

else:
	finalVals = []


	startWaitingForPoints = comm.bcast(mainProcess, root=0)
	#espero por la matriz de todos los puntos
	#normMatrix = comm.recv(source=0)

	while (starWaitingForPoints):
		pointsAvg = comm.recv(source=0) #Espero a que me manden un punto
		#procesa un punto
		if isinstance(point, bool):
			start = False
		else:
			add = 0
		    for value in pointsAvg:
		        add = add + value
	        avg = add/len(pointsAvg)		
		    if avg >= S:
		        finalVals.append([avg, longC, latC])
	comm.send(finalVals, dest=0)
