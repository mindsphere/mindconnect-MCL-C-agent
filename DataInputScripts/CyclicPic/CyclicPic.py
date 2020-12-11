
from time import sleep
from picamera import PiCamera
sleeptime = 3598


try:
	camera = PiCamera()
	camera.resolution = (320, 240)
	
	
	camera.start_preview(alpha = 128, fullscreen = False, window = (100,20,320,240))
	sleep(2)
	
	for filename in camera.capture_continuous('/tmp/RPiAgentFiles2Upload/img{counter:03d}.jpg'):
		camera.stop_preview()
		while (sleeptime >= 0) :
			print('\rCaptured', filename ,'sleeping ' ,sleeptime,end = '   \r' )
			sleep(1) # Wait sleeptime in s before next pic is taken
			sleeptime = sleeptime -1   
		sleeptime = 3598;
		camera.start_preview(alpha =  128, fullscreen = False, window = (100,20,320,240))
		sleep(2)
		
	

except KeyboardInterrupt: exit
    
