# coding=utf-8
# Requred Modules/Libraries 
import RPi.GPIO as GPIO
import time
import datetime
from time import sleep
from picamera import PiCamera

global Counter
global WriteNow
 
GPIO.setmode(GPIO.BCM)
 
# GPIO Pins of the installed sensors
PIN_CLK = 16
PIN_DT = 15
BUTTON_PIN = 14
 
GPIO.setup(PIN_CLK, GPIO.IN, pull_up_down = GPIO.PUD_UP)
GPIO.setup(PIN_DT, GPIO.IN, pull_up_down = GPIO.PUD_UP)
GPIO.setup(BUTTON_PIN, GPIO.IN, pull_up_down = GPIO.PUD_UP)
 
# Init of variables
Richtung = True
PIN_CLK_LETZTER = 0
PIN_CLK_AKTUELL = 0
PIN_DT_AKTUELL  = 0
delayTime = 0.1
CountedTime = 0
 
# First read of Pin_CLK
PIN_CLK_LETZTER = GPIO.input(PIN_CLK)
Counter = 0
WriteNow = 0
CounterNow = 0
 

# detecting rotation
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

		print ("")
		if Richtung:
			print ("Rotation: + ",Counter)
		else:
			print ("Rotation: - ",Counter)
	
       
 # Callback on pressing the button 
def CounterReset(null):
	global Counter
	global WriteNow
	global CounterNow
	
	print ("Button pressed -> Take pic , send Event , Position reset")
	print ("------------------------------")
	CounterNow = Counter
	Counter = 0
	WriteNow = 2 # trigger pic and event
 
# CallBack-Option of GPIO Python Modul 
GPIO.add_event_detect(PIN_CLK, GPIO.BOTH, callback=ausgabeFunktion, bouncetime=50)
GPIO.add_event_detect(BUTTON_PIN, GPIO.FALLING, callback=CounterReset, bouncetime=50)
 
 
print ("Rotary Encoder Data Input Script for RPiAgentV3 -> STRG+C, to stop")
 
# main loop
try:
	camera = PiCamera()
	camera.resolution = (320, 240)
	
	while True:

		time.sleep(delayTime)
		CountedTime = CountedTime + delayTime;
		print ("No rotation for "+str(int(CountedTime))+"s -> Position : "+str(Counter),end = '   \r' )
		
		if CountedTime > 10.0:
			print ("")
			CountedTime = 0
			WriteNow = 1	#trigger cyclic output of Data 
	
		# something to do , if WriteNow > 0 
		if WriteNow > 0 : 
			sztime = str(datetime.datetime.utcnow())
			szFileTime = time.strftime("%Y%m%d-%H%M%S")
			
			bOk = False
			while bOk == False:
				try:	
					#Data Input
					with open('/tmp/RPiAgentDataInput.txt','a') as f:
						f.write("1002|"+"0000000000|"+'{:>10.4f}'.format(Counter)       + "|"+ sztime+"\n")
					f.close()
					
					# Button pressed - > Generate Event & take picture
					if WriteNow == 2 : 	
						#Camera shot
						camera.start_preview(alpha = 128, fullscreen = False, window = (100,20,320,240))
						camera.capture("/tmp/RPiAgentFiles2Upload/"+szFileTime+"img.jpg") 
						camera.stop_preview()
					
						
						#Event
						with open('/tmp/RPiAgentEventInput.txt','a') as f:
							print("Event ID: 0001|"+ sztime+"|+ Position: ",CounterNow,"|n")
							f.write("0001|"+ sztime+"|+ Position: "+str(CounterNow)+"|n\n")
						f.close()

	
					WriteNow = 0
					CountedTime = 0
					bOk = True
				except IOError as error:
					time.sleep(0.001)
 
 
# Cleanup on finishing
except KeyboardInterrupt:
        GPIO.cleanup()
