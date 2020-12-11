# coding=utf-8
# Benötigte Module werden eingefügt und konfiguriert
import time
import datetime
import RPi.GPIO as GPIO


GPIO.setmode(GPIO.BCM)

# Hier können die jeweiligen Eingangs-/Ausgangspins ausgewählt werden
Trigger_AusgangsPin = 17
Echo_EingangsPin    = 27

# Die Pause zwischen den einzelnen Messugnen kann hier in Sekunden eingestellt werden
sleeptime = 2.0

# Hier werden die Ein-/Ausgangspins konfiguriert
#GPIO.setwarnings(False)
GPIO.setup(Trigger_AusgangsPin, GPIO.OUT)
GPIO.setup(Echo_EingangsPin, GPIO.IN)
GPIO.output(Trigger_AusgangsPin, False)
OldDauer = 0
 
# Hauptprogrammschleife
try:
	while True:
		# Abstandsmessung wird mittels des 10us langen Triggersignals gestartet
		GPIO.output(Trigger_AusgangsPin, True)
		time.sleep(0.00001)
		GPIO.output(Trigger_AusgangsPin, False)
		
		# Hier wird die Stopuhr gestartet
		EinschaltZeit = time.time()
		while GPIO.input(Echo_EingangsPin) == 0:
			EinschaltZeit = time.time() # Es wird solange die aktuelle Zeit gespeichert, bis das Signal aktiviert wird
 
		while GPIO.input(Echo_EingangsPin) == 1:
			AusschaltZeit = time.time() # Es wird die letzte Zeit aufgenommen, wo noch das Signal aktiv war
		
		# Die Differenz der beiden Zeiten ergibt die gesuchte Dauer
		Dauer = AusschaltZeit - EinschaltZeit
		# Mittels dieser kann nun der Abstand auf Basis der Schallgeschwindigkeit der Abstand berechnet werden
		Abstand = (Dauer * 34300) / 2
		
		# Überprüfung, ob der gemessene Wert innerhalb der zulässigen Entfernung liegt
		if Abstand < 1 or (round(Abstand) > 3000):
			Dauer = OldDauer
			# Falls nicht wird eine Fehlermeldung ausgegeben
			print("Distance invalid")
		else:
			# Der Abstand wird auf zwei Stellen hinterm Komma formatiert
			Abstand = format((Dauer * 34300) / 2, '.2f')
			# Der berechnete Abstand wird auf der Konsole ausgegeben
			OldDauer = Dauer
			print (Abstand, "cm")
			
		sztime = str(datetime.datetime.utcnow())
		
		bOk = False
		while bOk == False:
			try:	
				with open('/tmp/RPiAgentDataInput.txt','a') as f:
					f.write("2001|"+"0000000000|"+'{:>10.4f}'.format((Dauer * 34300) / 2) + "|"+ sztime+"\n")
					f.close()
					bOk = True
			except IOError as error:
				time.sleep(0.001)

		# Pause zwischen den einzelnen Messungen
		time.sleep(sleeptime)
 
# Aufraeumarbeiten nachdem das Programm beendet wurde
except KeyboardInterrupt:
	GPIO.cleanup()
