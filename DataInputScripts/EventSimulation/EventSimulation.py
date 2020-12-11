# coding=utf-8
# Benoetigte Module werden importiert und eingerichtet
import glob
import time
import datetime
import math
from time import sleep
import RPi.GPIO as GPIO


# An dieser Stelle kann die Pause zwischen den einzelnen Messungen eingestellt werden
sleeptime = 1
coming = 1



# Hauptprogrammschleife

print ("Event Generation  each "+str(sleeptime)+"s   ------------------")
try:
    
	while True:
        
        
        
		sztime = str(datetime.datetime.utcnow())

		bOk = False
		while bOk == False:
			try:	
				with open('/tmp/RPiAgentEventInput.txt','a') as f:
					if coming == 1:
						print("0001|"+ sztime+"|+|n")
						f.write("0001|"+ sztime+"|+|n\n")
						coming = 0
					else:
						print("0001|"+ sztime+"|-|n")
						f.write("0001|"+ sztime+"|-|n\n")
						coming = 1
					f.close()
					bOk = True
			except IOError as error:
				time.sleep(0.001)
               
		time.sleep(sleeptime)

except KeyboardInterrupt: exit
    
