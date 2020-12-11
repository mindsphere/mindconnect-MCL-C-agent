Agent for MindSphere based on Mindconnect Library V4 and written in C
Compiled and tested on Raspberry Pi V4 with Raspbian Buster.

Principle: Raw Values are coming from data sources (sensors) that are obtained by data acqisition scripts written in python. The data are written in a file interface. 
The agent reads cyclically the file interface and transfers the data to mindsphere.
Using the file interface you can connect any possible data source like proprietary protocols or OPC or whatever ... 
How to write in the file interface see the documentation or check the python scripts attached.
The agent is reading a config file on start , containing all required informations like tenant name , certifcates etc...

Basic description based on the usage on the RPi see "RPi Demo Case Instructions V4.2.docx"

Used IDE : CodeBlocks 17.12 ( V20 may also work ...?)
http://www.codeblocks.org/downloads/5

Required Libraries:

Mindconnect Libraries with dependant libs like CURL , OpenSSL
on Github : 
https://github.com/mindsphere/mindconnect-lib
via MindSphere.io
https://developer.mindsphere.io/resources/mindconnect-lib-v4/resources-mclib-overview.html

Libconfig for reading and writing the Config file:
https://hyperrealm.github.io/libconfig/

Any questions ? 
Horst.Rieger@Siemens.Com
