# coding=utf-8
# Benoetigte Module werden importiert und eingerichtet
import RPi.GPIO as GPIO
import time
import datetime

global Counter
global WriteNow
 
GPIO.setmode(GPIO.BCM)
 
# Hier werden die Eingangs-Pins deklariert, an dem der Sensor angeschlossen ist.
PIN_CLK = 16
PIN_DT = 15
BUTTON_PIN = 14
 
GPIO.setup(PIN_CLK, GPIO.IN, pull_up_down = GPIO.PUD_UP)
GPIO.setup(PIN_DT, GPIO.IN, pull_up_down = GPIO.PUD_UP)
GPIO.setup(BUTTON_PIN, GPIO.IN, pull_up_down = GPIO.PUD_UP)
 
# Benötigte Variablen werden initialisiert
Richtung = True
PIN_CLK_LETZTER = 0
PIN_CLK_AKTUELL = 0
PIN_DT_AKTUELL  = 0
delayTime = 0.01
CountedTime = 0
 
# Initiales Auslesen des Pin_CLK
PIN_CLK_LETZTER = GPIO.input(PIN_CLK)
Counter = 0
WriteNow = 0
 

# Diese AusgabeFunktion wird bei Signaldetektion ausgefuehrt
def ausgabeFunktion(null):
	global Counter
	global WriteNow
	
	PIN_DT_AKTUELL = GPIO.input(PIN_DT)
	PIN_CLK_AKTUELL = GPIO.input(PIN_CLK)
	
	WriteNow = 1
 
	if PIN_CLK_AKTUELL != PIN_CLK_LETZTER:
		
		if  PIN_DT_AKTUELL!= PIN_CLK_AKTUELL:
			Counter = Counter + 1
			Richtung = True;
		else:
			Richtung = False
			Counter = Counter - 1

		
		if Richtung:
			print ("Rotation: + ",Counter)
		else:
			print ("Rotation: - ",Counter)
	
       
 
def CounterReset(null):
	global Counter
	global WriteNow
	
	print ("Position reset")
	print ("------------------------------")
	Counter = 0
	WriteNow = 1
 
# Um einen Debounce direkt zu integrieren, werden die Funktionen zur Ausgabe mittels
# CallBack-Option vom GPIO Python Modul initialisiert
GPIO.add_event_detect(PIN_CLK, GPIO.BOTH, callback=ausgabeFunktion, bouncetime=50)
GPIO.add_event_detect(BUTTON_PIN, GPIO.FALLING, callback=CounterReset, bouncetime=50)
 
 
print ("STRG+C, to stop")
 
# Hauptprogrammschleife
try:
	
	
	while True:

		time.sleep(delayTime)
		CountedTime = CountedTime + delayTime;
		
		if CountedTime > 10.0:
			print ("no rotation for 10s -> Position : ",  Counter)
			CountedTime = 0
			WriteNow = 1
	
		if WriteNow == 1 : 
			sztime = str(datetime.datetime.now())
			
			bOk = False
			while bOk == False:
				try:	
					with open('/tmp/RPiAgentDataInput.txt','a') as f:
						f.write("2001|"+"0000000000|"+'{:>10.4f}'.format(Counter)       + "|"+ sztime+"\n")
						f.close()
						WriteNow = 0
						CountedTime = 0
						bOk = True
				except IOError as error:
					time.sleep(0.001)
 
 
# Aufraeumarbeiten nachdem das Programm beendet wurde
except KeyboardInterrupt:
        GPIO.cleanup()
