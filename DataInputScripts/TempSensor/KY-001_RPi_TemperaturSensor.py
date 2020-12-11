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
sleeptime = 1

# Der One-Wire EingangsPin wird deklariert und der integrierte PullUp-Widerstand aktiviert
GPIO.setmode(GPIO.BCM)
GPIO.setup(4, GPIO.IN, pull_up_down=GPIO.PUD_UP)

# Nach Aktivierung des Pull-UP Widerstandes wird gewartet,
# bis die Kommunikation mit dem DS18B20 Sensor aufgebaut ist
print ("Warte auf Initialisierung...")

base_dir = '/sys/bus/w1/devices/'
while True:
	try:
		device_folder = glob.glob(base_dir + '28*')[0]
		break
	except IndexError:
		sleep(0.2)
		continue
device_file = device_folder + '/w1_slave'


# Funktion wird definiert, mit dem der aktuelle Messwert am Sensor ausgelesen werden kann
def TemperaturMessung():
	f = open(device_file, 'r')
	if  (f):
		lines = f.readlines()
	f.close()
	return lines

# Zur Initialisierung, wird der Sensor einmal "blind" ausgelesen
TemperaturMessung()

# Die Temperaturauswertung: Beim Raspberry Pi werden erkennte one-Wire Slaves im Ordner
# /sys/bus/w1/devices/ einem eigenen Unterordner zugeordnet. In diesem Ordner befindet sich die Datei w1-slave
# in dem Die Daten, die über dem One-Wire Bus gesendet wurden gespeichert.
# In dieser Funktion werden diese Daten analysiert und die Temperatur herausgelesen und ausgegeben
def TemperaturAuswertung():
	lines = TemperaturMessung()
	while lines[0].strip()[-3:] != 'YES':
		time.sleep(0.1)
		lines = TemperaturMessung()
	equals_pos = lines[1].find('t=')
	if equals_pos != -1:
		temp_string = lines[1][equals_pos+2:]
		temp_c = float(temp_string) / 1000.0
		return temp_c

# Hauptprogrammschleife
# Die gemessene Temperatur wird in die Konsole ausgegeben - zwischen den einzelnen Messungen
# ist eine Pause, deren Länge mit der Variable "sleeptime" eingestellt werden kann
try:
    
	while True:
		print ('---------------------------------------')
		tempc=TemperaturAuswertung()
		#sztempc = str(tempc)
		#sztempc = ':<10'.format(tempc)
		sztime = str(datetime.datetime.now())
		print ("Temperatur:", tempc, "°C - ", sztime)
        
		sztime = str(datetime.datetime.utcnow())
		bOk = False
		while bOk == False:
			try:	
				with open('/tmp/RPiAgentDataInput.txt','a') as f:
					f.write("1001|"+"0000000000|"+'{:>10.4f}'.format(tempc)       + "|"+ sztime+"\n")
					#f.write("1002|"+"0000000000|"+'{:>10.4f}'.format((tempc)*10)  + "|"+ sztime+"\n")
					#f.write("1002|"+"0000000000|"+'{:>10.4f}'.format(math.sin(fxSin))  + "|"+ sztime+"\n")
					#f.write("2001|"+"0000000000|"+'{:>10.4f}'.format((tempc)*0.1) + "|"+ sztime+"\n")
					#f.write("2001|"+"0000000000|"+'{:>10.4f}'.format(((fxSin))*10) + "|"+ sztime+"\n")
					f.close()
					bOk = True
			except IOError as error:
				time.sleep(0.001)

		fxSin += 0.01
		if fxSin >= 4*math.pi:	fxSin = 0
       
		time.sleep(sleeptime)

except KeyboardInterrupt:
    GPIO.cleanup()
