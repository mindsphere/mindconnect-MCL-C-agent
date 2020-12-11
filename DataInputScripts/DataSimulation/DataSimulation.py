# coding=utf-8
# Benoetigte Module werden importiert und eingerichtet
import glob
import time
import datetime
import math
from time import sleep
import RPi.GPIO as GPIO

fxSin = 0

# An dieser Stelle kann die Pause zwischen den einzelnen Messungen eingestellt werden
sleeptime = 10


print ("Init Datasimulation with sin ,cos ,sawtooth ...")


# Hauptprogrammschleife

# ist eine Pause, deren Länge mit der Variable "sleeptime" eingestellt werden kann
print ('  x  sinx cosx x*10  ------------------')
try:
    
	while True:
        
        
        
		sztime = str(datetime.datetime.utcnow())
		print ('{:>3.2f}'.format(fxSin),'{:>3.2f}'.format(math.sin(fxSin)),'{:>3.2f}'.format(math.cos(fxSin)),'{:>3.2f}'.format(((fxSin))*10), " - ", sztime)
        
		bOk = False
		while bOk == False:
			try:	
				with open('/tmp/RPiAgentDataInput.txt','a') as f:
					f.write("1001|"+"0000000000|"+'{:>10.4f}'.format(math.sin(fxSin))       + "|"+ sztime+"\n")
					f.write("1002|"+"0000000000|"+'{:>10.4f}'.format(math.cos(fxSin))  + "|"+ sztime+"\n")
					f.write("2001|"+"0000000000|"+'{:>10.4f}'.format(((fxSin))*10) + "|"+ sztime+"\n")
					f.close()
					bOk = True
			except IOError as error:
				time.sleep(0.001)
				gotoline (34)
        
                
		fxSin += 0.01
		if fxSin >= 4*math.pi:	
			fxSin = 0
               
		time.sleep(sleeptime)

except KeyboardInterrupt: exit
    
