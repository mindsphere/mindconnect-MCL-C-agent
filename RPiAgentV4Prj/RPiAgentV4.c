
#include <ctype.h>
#include <libconfig.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <linux/input.h>
#include <signal.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>

#include "mcl_core/mcl_core.h"
#include "mcl_core/mcl_core_configuration.h"

#include "mcl_core/mcl_random.h"
#include "mcl_core/mcl_json_util.h"

#include "mcl_connectivity/mcl_connectivity.h"
#include "mcl_data_lake/mcl_data_lake.h"
#include "callbacks.h"
#include "mcl_core/mcl_log_util.h"
#include "mcl_core/mcl_file_util.h"





//static void _setup_configuration(mcl_core_configuration_t *configuration);
//static E_MCL_CORE_RETURN_CODE _add_data_model(char **configuration_id);


#define MAXDP 1000
#define MAXDS 10
//#define MAXDPTS 100						// Max 100 Values per upload des Data Stores
#define MAXEV 1000						// Max 1000 Events


char *szRpiAgentVersion = "4.21.000";
// 3.00.002 Datentypen Check in Config. Typ boolean f�r 0/1/und true/false als input enabled
// 3.10.002 Memory Leak in ManageFileupload fixed - closedir missing
// 3.10.003  14.09.18 %s in description of event to include parameter pC from python script
// 3.10.004  19.11.18 Payload size nur f�r timeseries auf max 1MB begrenzt, das ist das max was MDsph akzeptiert. Files upload bis 10MB.
// 3.10.005  31.01.19 Check of Input lines improved , empty or invalid mow are ignored, no crash anymore.
// 3.10.006	 11.09.19 endless retry of datastore transfer if internet connection is broken.
// 4.00.000	 24.01.20 Migration to MCL V4
// 4.21.000  07.10.20 New functionality of MCL V4.2

//some global vars
int nDataPoints = 0;
mcl_core_t *core = NULL;// initialize a new connectivity with given configuration
mcl_connectivity_t *connectivity = NULL;
// Connectivity configuration and related parameter.
mcl_connectivity_configuration_t *connectivity_configuration = NULL;
mcl_core_configuration_t *core_configuration = NULL;
mcl_data_source_configuration_t *data_source_configuration = NULL;
mcl_data_source_t *data_source = NULL;

int nAgentStartDelay = 10;
int nAgentSleepTime = 10; // in s
int TimeStamp_Precision	= 0;	// 0 = max. = x.yyy s;  1 = x.yy0;  2 = x.y00;  3 = x.000 s

char *szDataInputFile;
char *szEventInputFile;


long lDataPoints 		= 0;
long lDataPointsSent 	= 0;
long lDataPointsSentOk 	= 0;
long lUploadsSentOk 	= 0;
long lUploadsSent 		= 0;
long lRotateKeys		= 0;
long lServerError		= 0;



// Config File Globals
char *config_file;
config_t cfg;
config_setting_t *CfgRoot, *CfgGroup , *CfgMember;
int bWrite = 0;


char* szCfg_MdSphUrl 				= "https:// Your MindSphere V3 Base Url ";
int port 							= 443;
int security_profile 				= MCL_SECURITY_SHARED_SECRET;
char* szproxy_hostname 				= "";// NULL;
int   proxy_port	 				= 0;
int   proxy_type	  	 			= MCL_PROXY_HTTP;
char* szproxy_username 				= "";//NULL;
char* szproxy_password 				= "";//NULL;
char* szproxy_domain 				= "";//NULL;
int   max_http_payload_size 		= 1024*1024*10; // 10 MByte
int   http_request_timeout			= 300; //sec
char* szuser_agent 					= "custom agent v1.0";
char* szinitial_access_token 		= "Get the IAT from MindSphere and put in here";
char* sztenant 						= "Your MindSphere tenant name";
char* szstore_path;
char* szRegistrationFile;
char  szPrintOut[5000];




int   nDebugLevel = 4; // Fuer MCL Debug
char* szAgentDebugFile ; // Fuer MCL Debug , printf redirecten / duplizieren agentprintf bauen mit zusätzlicher Ausgabe in File
char* szMclDebugFile;

// existenz von Dirs / Files ueberpruefen
// Wiederholen bei Http fehlern

char* szCert 					= "-----BEGIN CERTIFICATE-----\n"\
		"MIIFYDCCA0igAwIBAgIURFc0JFuBiZs18s64KztbpybwdSgwDQYJKoZIhvcNAQEL\n"\
		"BQAwSDELMAkGA1UEBhMCQk0xGTAXBgNVBAoTEFF1b1ZhZGlzIExpbWl0ZWQxHjAc\n"\
		"BgNVBAMTFVF1b1ZhZGlzIFJvb3QgQ0EgMiBHMzAeFw0xMjAxMTIxODU5MzJaFw00\n"\
		"MjAxMTIxODU5MzJaMEgxCzAJBgNVBAYTAkJNMRkwFwYDVQQKExBRdW9WYWRpcyBM\n"\
		"aW1pdGVkMR4wHAYDVQQDExVRdW9WYWRpcyBSb290IENBIDIgRzMwggIiMA0GCSqG\n"\
		"SIb3DQEBAQUAA4ICDwAwggIKAoICAQChriWyARjcV4g/Ruv5r+LrI3HimtFhZiFf\n"\
		"qq8nUeVuGxbULX1QsFN3vXg6YOJkApt8hpvWGo6t/x8Vf9WVHhLL5hSEBMHfNrMW\n"\
		"n4rjyduYNM7YMxcoRvynyfDStNVNCXJJ+fKH46nafaF9a7I6JaltUkSs+L5u+9ym\n"\
		"c5GQYaYDFCDy54ejiK2toIz/pgslUiXnFgHVy7g1gQyjO/Dh4fxaXc6AcW34Sas+\n"\
		"O7q414AB+6XrW7PFXmAqMaCvN+ggOp+oMiwMzAkd056OXbxMmO7FGmh77FOm6RQ1\n"\
		"o9/NgJ8MSPsc9PG/Srj61YxxSscfrf5BmrODXfKEVu+lV0POKa2Mq1W/xPtbAd0j\n"\
		"IaFYAI7D0GoT7RPjEiuA3GfmlbLNHiJuKvhB1PLKFAeNilUSxmn1uIZoL1NesNKq\n"\
		"IcGY5jDjZ1XHm26sGahVpkUG0CM62+tlXSoREfA7T8pt9DTEceT/AFr2XK4jYIVz\n"\
		"8eQQsSWu1ZK7E8EM4DnatDlXtas1qnIhO4M15zHfeiFuuDIIfR0ykRVKYnLP43eh\n"\
		"vNURG3YBZwjgQQvD6xVu+KQZ2aKrr+InUlYrAoosFCT5v0ICvybIxo/gbjh9Uy3l\n"\
		"7ZizlWNof/k19N+IxWA1ksB8aRxhlRbQ694Lrz4EEEVlWFA4r0jyWbYW8jwNkALG\n"\
		"cC4BrTwV1wIDAQABo0IwQDAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIB\n"\
		"BjAdBgNVHQ4EFgQU7edvdlq/YOxJW8ald7tyFnGbxD0wDQYJKoZIhvcNAQELBQAD\n"\
		"ggIBAJHfgD9DCX5xwvfrs4iP4VGyvD11+ShdyLyZm3tdquXK4Qr36LLTn91nMX66\n"\
		"AarHakE7kNQIXLJgapDwyM4DYvmL7ftuKtwGTTwpD4kWilhMSA/ohGHqPHKmd+RC\n"\
		"roijQ1h5fq7KpVMNqT1wvSAZYaRsOPxDMuHBR//47PERIjKWnML2W2mWeyAMQ0Ga\n"\
		"W/ZZGYjeVYg3UQt4XAoeo0L9x52ID8DyeAIkVJOviYeIyUqAHerQbj5hLja7NQ4n\n"\
		"lv1mNDthcnPxFlxHBlRJAHpYErAK74X9sbgzdWqTHBLmYF5vHX/JHyPLhGGfHoJE\n"\
		"+V+tYlUkmlKY7VHnoX6XOuYvHxHaU4AshZ6rNRDbIl9qxV6XU/IyAgkwo1jwDQHV\n"\
		"csaxfGl7w/U2Rcxhbl5MlMVerugOXou/983g7aEOGzPuVBj+D77vfoRrQ+NwmNtd\n"\
		"dbINWQeFFSM51vHfqSYP1kjHs6Yi9TM3WpVHn3u6GBVv/9YUZINJ0gpnIdsPNWNg\n"\
		"KCLjsZWDzYWm3S8P52dSbrsvhXz1SnPnxT7AvSESBT/8twNJAlvIJebiVDj1eYeM\n"\
		"HVOyToV7BjjHLPj4sHKNJeV3UvQDHEimUF+IIDBu8oJDqz2XhOdT+yHBTw8imoa4\n"\
		"WSr2Rz0ZiC3oheGe7IUIarFsNMkd7EgrO3jtZsSOeWmD3n+M\n"\
		"-----END CERTIFICATE-----";

char *szData_Lake_Certificate = "-----BEGIN CERTIFICATE-----\n" \
"MIIDdzCCAl+gAwIBAgIEAgAAuTANBgkqhkiG9w0BAQUFADBaMQswCQYDVQQGEwJJ\n" \
"RTESMBAGA1UEChMJQmFsdGltb3JlMRMwEQYDVQQLEwpDeWJlclRydXN0MSIwIAYD\n" \
"VQQDExlCYWx0aW1vcmUgQ3liZXJUcnVzdCBSb290MB4XDTAwMDUxMjE4NDYwMFoX\n" \
"DTI1MDUxMjIzNTkwMFowWjELMAkGA1UEBhMCSUUxEjAQBgNVBAoTCUJhbHRpbW9y\n" \
"ZTETMBEGA1UECxMKQ3liZXJUcnVzdDEiMCAGA1UEAxMZQmFsdGltb3JlIEN5YmVy\n" \
"VHJ1c3QgUm9vdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKMEuyKr\n" \
"mD1X6CZymrV51Cni4eiVgLGw41uOKymaZN+hXe2wCQVt2yguzmKiYv60iNoS6zjr\n" \
"IZ3AQSsBUnuId9Mcj8e6uYi1agnnc+gRQKfRzMpijS3ljwumUNKoUMMo6vWrJYeK\n" \
"mpYcqWe4PwzV9/lSEy/CG9VwcPCPwBLKBsua4dnKM3p31vjsufFoREJIE9LAwqSu\n" \
"XmD+tqYF/LTdB1kC1FkYmGP1pWPgkAx9XbIGevOF6uvUA65ehD5f/xXtabz5OTZy\n" \
"dc93Uk3zyZAsuT3lySNTPx8kmCFcB5kpvcY67Oduhjprl3RjM71oGDHweI12v/ye\n" \
"jl0qhqdNkNwnGjkCAwEAAaNFMEMwHQYDVR0OBBYEFOWdWTCCR1jMrPoIVDaGezq1\n" \
"BE3wMBIGA1UdEwEB/wQIMAYBAf8CAQMwDgYDVR0PAQH/BAQDAgEGMA0GCSqGSIb3\n" \
"DQEBBQUAA4IBAQCFDF2O5G9RaEIFoN27TyclhAO992T9Ldcw46QQF+vaKSm2eT92\n" \
"9hkTI7gQCvlYpNRhcL0EYWoSihfVCr3FvDB81ukMJY2GQE/szKN+OMY3EU/t3Wgx\n" \
"jkzSswF07r51XgdIGn9w/xZchMB5hbgF/X++ZRGjD8ACtPhSNzkE1akxehi/oCr0\n" \
"Epn3o0WC4zxe9Z2etciefC7IpJ5OCBRLbf1wbWsaY71k5h+3zvDyny67G7fyUIhz\n" \
"ksLi4xaNmjICq44Y3ekQEe5+NauQrz4wlHrQMz2nZQ/1/I6eYs9HRCwBXbsdtTLS\n" \
"R9I4LtD+gdwyah617jzV/OeBHRnDJELqYzmp\n" \
"-----END CERTIFICATE-----";


 char*  szTimeSeriesVersion 	= "1.0";
 char*  szDataSourceVersion 	= "1.0";


 int	nEvent_Count = 1;

 int    nDataSrc_Count = 2;
 int    nDataSrc_DataPointsCount[MAXDS];
 char *szDataConfig_ID = "WillBeDoneByAgent";
 char **szDataSrc_Description;
 char **szDataSrc_Name;
 char **szdscf;
 char **szdscv;
 char **szdpcf;
 char **szdpcv;
 char **szDataPt_Description;
 char **szDataPt_ID; //[100];
 char **szDataPt_Name;//[100];
 char **szDataPt_DataType;//[20];
 char **szDataPt_Unit;//[100];
 char **szMinDeltaValue;  		// Der Wert bei dem eine neuer Datenpunkt gesendet wird entweder in % oder absolut. zulssig zb 5 oder 5% Unterscheidung am % Zeichen
 char **szDataPt_Map2AssetID; // Asset ID , dem die Daten gehören bzw in dem das MCL Agent Asset Plugin enthalten ist
 char **szDataPt_Map2AspectSet;
 char **szDataPt_Map2AspectVar;
 char **szDataPt_MapStatus;

 float fMinDeltaValue[MAXDP];
 int   nMaxDeltaTime[MAXDP]; 	// Die Zeit nach der spaetestens ein neuer Datenpunkt Wert übermittelt wird, wenn er auch sich nicht geaendert hat. 0= max 1= 10ms 2 = 100ms etc
 int   nDataPtInCfg[MAXDP];
 int   nDataPtKeepMapping[MAXDP];

 char **szEvent_Source;
 char **szEvent_Description;
 int    Event_Severity[MAXEV];

 char *szUpLoadDir;
 char *szUpLoadDirDL;
 char *szSubTenantDL;
 char *szUpLoadUrlDL;
 long nSleepPts = 0;
 int nNewDataModelCreated = 0;
 //char **szEvent_CID;

 char* strlwr(char* s)
 {
     char* tmp = s;

     for (;*tmp;++tmp) {
         *tmp = tolower((unsigned char) *tmp);
     }

     return s;
 }

 char* strupr(char* s)
 {
     char* tmp = s;

     for (;*tmp;++tmp) {
         *tmp = toupper((unsigned char) *tmp);
     }

     return s;
 }

 void chknfree(void* ptr)
 {
    if (ptr != NULL)
        free(ptr);
 }

 void cleanup(void)
 {
 	int j;

 	chknfree (config_file);		chknfree (szstore_path );   chknfree (szDataInputFile);  	chknfree (szRegistrationFile);


 	// DataSources
 	for (j = 0; j < MAXDS; j++)
 	{
 		chknfree(szDataSrc_Description[j]);
 		chknfree(szDataSrc_Name[j]);
 		chknfree(szdscf[j]);
 		chknfree(szdscv[j]);
 	}
 	chknfree(szDataSrc_Description);
 	chknfree(szDataSrc_Name);
	chknfree(szdscf);
	chknfree(szdscv);


 	// DataPoints
 	for (j = 0; j < MAXDP; j++)
 	{
 		 chknfree(szDataPt_ID[j]);
 		 chknfree(szDataPt_Name[j]);
 		 chknfree(szDataPt_Description[j]);
 		 chknfree(szDataPt_DataType[j]);
 		 chknfree(szDataPt_Unit[j]);
 		 chknfree(szMinDeltaValue[j]);
 		 chknfree(szdpcf[j]);
 		 chknfree(szdpcv[j]);
 		 chknfree(szDataPt_Map2AssetID[j]);
 		 chknfree(szDataPt_Map2AspectSet[j]);
 		 chknfree(szDataPt_Map2AspectVar[j]);
 		 chknfree(szDataPt_MapStatus[j]);
 	}

 	chknfree(szDataPt_ID);
 	chknfree(szDataPt_Name);
 	chknfree(szDataPt_Description);
 	chknfree(szDataPt_DataType);
 	chknfree(szDataPt_Unit);
 	chknfree(szMinDeltaValue);
 	chknfree(szdpcf);
    chknfree(szdpcv);
    chknfree(szDataPt_Map2AssetID);
    chknfree(szDataPt_Map2AspectSet);
 	chknfree(szDataPt_Map2AspectVar);
    chknfree(szDataPt_MapStatus);
 	// Events
 	for (j = 0; j < MAXEV; j++)
 	{

 		chknfree(szEvent_Source[j]);
 		chknfree(szEvent_Description[j]);
 		//chknfree(szEvent_CID[j]);

	}
 	chknfree(szEvent_Source);
 	chknfree(szEvent_Description);
 	chknfree(szUpLoadDirDL);
 	chknfree(szUpLoadUrlDL);
 	chknfree(szSubTenantDL);

 	//chknfree(szEvent_CID);


 }
 int bPrintfAgent = 1;

 static void  MCLdprint (char *szDebugString)
{

	if (szDebugString == NULL)
		return;

	if (bPrintfAgent)
        printf(szDebugString);

	if (szMclDebugFile == NULL)
		return;

	if (!strlen (szMclDebugFile))
		return;

	FILE* fp;
	if ((fp = fopen(szMclDebugFile,"a")) != NULL)
	{
		fputs(szDebugString, fp);
		fclose(fp);
	}
}
static void  agentdprint (char *szDebugString)
{

	if (szDebugString == NULL)
		return;

	printf(szDebugString);

	bPrintfAgent = 0;
	MCLdprint (szDebugString);
	bPrintfAgent = 1;

	if (szAgentDebugFile == NULL)
		return;

	if (!strlen (szAgentDebugFile))
		return;

	FILE* fp;
	if ((fp = fopen(szAgentDebugFile,"a")) != NULL)
	{
		fputs(szDebugString, fp);
		fclose(fp);
	}
}

static void log_callback(void *user_context, int log_level, const char *file, int line, const char *tag, const char * const format, ...);
mcl_log_util_callback_t log_function = log_callback;
void *mcl_log_util_user_context = NULL;

static void log_callback(void *user_context, int log_level, const char *file, int line, const char *tag, const char * const format, ...)
{


    static const char *_mcl_level_strings[] =
    {
        "VERBOSE",
        "DEBUG",
        "INFO",
        "WARN",
        "ERROR",
        "FATAL"
    };

char sztmp [5000];
    va_list args;
    va_start(args, format);

    // user_context will not be used.
    (void) user_context;

    sprintf(szPrintOut,"MCL | %s | %s | %d | %s | ", _mcl_level_strings[log_level - MCL_LOG_LEVEL_VERBOSE], file, line, tag);
    vsprintf(sztmp,format, args);
    strcat(szPrintOut, sztmp);
    strcat(szPrintOut, "\n");
    MCLdprint(szPrintOut);
    va_end(args);
}
static void GetSetConfigString(char *szEntry, char **szVal, config_setting_t * CfgGroup, int *pbWrite, int bCheck)
{

	config_setting_t *setting;
	int err = 0;
	char * cptr = NULL;

	if (NULL != (setting = config_setting_add(CfgGroup, szEntry, CONFIG_TYPE_STRING)))
				  {config_setting_set_string(setting, (const char*)*szVal); (*pbWrite)++;} // wenn entry nicht vorhanden -> erzeugen
			  else
				  config_setting_lookup_string(CfgGroup, szEntry,(const char**)szVal); // -> falls vorhanden , den ausgelesenen Wert �bernehmen

	if (*szVal != NULL)
	{
		if (strlen (*szVal) == 0)
		{
			*szVal = NULL;
			bCheck = 0;
		}
	}

	if (*szVal == NULL)
	{
		bCheck = 0;

    }

	if (bCheck) // Pr�fung auf unzul�ssige Zeichen, falls  bCheck == TRUE
	{
		int i, n;

		n = strlen(*szVal);
		cptr = (*szVal);

		for (i = 0 ; i < n ; i++, cptr++)
		{

			if ( ( isalnum (*cptr)))
				continue;


			switch (*cptr)
			{
				case ' ': break;
				case '-': break;
				//case '_': break;
				default: err++;
				sprintf(szPrintOut,"CfgErr: %30s: %s -> Illegal char: (%c)\n",szEntry, *szVal, *cptr); agentdprint (szPrintOut);
			}
		}
	}

	if (!err)
	{
		int bfree = 0;
		if (*szVal != NULL)
		{
			cptr = malloc((strlen (*szVal))+10);
			strcpy (cptr, *szVal);
			bfree++;
		}
		else
			cptr = "";


		sprintf(szPrintOut,"Config: %30s: %s\n",szEntry, cptr);
		agentdprint (szPrintOut);

		 if (bfree) chknfree (cptr);
	}

	/*
	if (*szVal == NULL)
	{
		*szVal = malloc(2);
		*szVal[0] = 0;
    }
*/
}

static void GetSetConfigInt(char *szEntry, int *nVal, config_setting_t * CfgGroup, int *pbWrite)
{

	config_setting_t *setting;// *CfgGroup;

	if (NULL != (setting = config_setting_add(CfgGroup, (const char*)szEntry, CONFIG_TYPE_INT)))
				  {config_setting_set_int(setting, *nVal); (*pbWrite)++;}							// wenn entry nicht vorhanden -> erzeugen
			  else
				  config_setting_lookup_int(CfgGroup, (const char*)szEntry,nVal);					// -> falls vorhanden , den ausgelesenen Wert �bernehmen

	sprintf(szPrintOut,"Config: %30s: %d\n",szEntry, *nVal); agentdprint (szPrintOut);

}


/*
static void GetSetConfigFloat(char *szEntry, double *dVal, config_setting_t * CfgGroup, int *pbWrite)
{

	config_setting_t *setting;// *CfgGroup;

	if (NULL != (setting = config_setting_add(CfgGroup, (const char*)szEntry, CONFIG_TYPE_FLOAT)))
				  {config_setting_set_int(setting, *dVal); (*pbWrite)++;}
			  else
				  config_setting_lookup_float(CfgGroup, (const char*)szEntry,dVal);

	sprintf(szPrintOut,"Config: %30s: %f\n",szEntry, *dVal);agentdprint (szPrintOut);

}
*/



static void ReadAgentConfiguration(void)
 {
	int j;
	int i,ii, err;

	err = 0;

		// DataSources
		szDataSrc_Description 		= (char **) malloc(MAXDS * sizeof(char*));
		szDataSrc_Name				= (char **) malloc(MAXDS * sizeof(char*));
		szdscf      				= (char **) malloc(MAXDS * sizeof(char*)); // custom Datasource field
		szdscv      				= (char **) malloc(MAXDS * sizeof(char*)); // custom Datasource value

		for (j = 0; j < MAXDS; j++)
		{
			nDataSrc_DataPointsCount[j] = 0;
			szDataSrc_Description[j] 	= (char*) malloc (100);	sprintf(szDataSrc_Description[j]	, "ToBeDone DataSrc Description %d",j+1);
			szDataSrc_Name[j]			= (char*) malloc (100); sprintf(szDataSrc_Name[j]			, "ToBeDone DataSrc Name %d",j+1);
			szdscf[j]		        	= (char*) malloc (100); sprintf(szdscf[j]		    	    , "Optional DataSrc %d Custom Field",j+1);
			szdscv[j]		        	= (char*) malloc (100); sprintf(szdscv[j]		    	    , "Optional DataSrc %d Custom Value",j+1);
		}

		// DataPoints
		szDataPt_ID 		    = (char **) malloc(MAXDP * sizeof(char*));
		szDataPt_Name		    = (char **) malloc(MAXDP * sizeof(char*));
		szDataPt_DataType	    = (char **) malloc(MAXDP * sizeof(char*));
		szDataPt_Unit		    = (char **) malloc(MAXDP * sizeof(char*));
		szMinDeltaValue		    = (char **) malloc(MAXDP * sizeof(char*));
		szDataPt_Description    = (char **) malloc(MAXDP * sizeof(char*));
		szdpcf      		    = (char **) malloc(MAXDP * sizeof(char*)); // Data point custom field
		szdpcv      		    = (char **) malloc(MAXDP * sizeof(char*));
		szDataPt_Map2AssetID    = (char **) malloc(MAXDP * sizeof(char*));
        szDataPt_Map2AspectSet  = (char **) malloc(MAXDP * sizeof(char*));
        szDataPt_Map2AspectVar  = (char **) malloc(MAXDP * sizeof(char*));
        szDataPt_MapStatus      = (char **) malloc(MAXDP * sizeof(char*));

		for (j = 0; j < MAXDP; j++)
	 	{
	 		 szDataPt_ID[j]			= (char*) malloc (30);  sprintf(szDataPt_ID[j]			, "ToBeDone DataPt ID %d",j+1);
	 		 szDataPt_Name[j]		= (char*) malloc (100); sprintf(szDataPt_Name[j]		, "ToBeDone DataPt Name %d",j+1);
	 		 szDataPt_DataType[j]	= (char*) malloc (30);  sprintf(szDataPt_DataType[j]	, "ToBeDone DataPt DataType %d",j+1);
	 		 szDataPt_Unit[j]		= (char*) malloc (30);  sprintf(szDataPt_Unit[j]		, "ToBeDone DataPt Unit %d",j+1);
	 		 szDataPt_Description[j]= (char*) malloc (100); sprintf(szDataPt_Description[j]	, "ToBeDone DataPt Description %d",j+1);
	 		 szdpcf[j]		       	= (char*) malloc (100); sprintf(szdpcf[j]		        , "Optional DataPt %d Custom Field",j+1);
             szdpcv[j]		       	= (char*) malloc (100); sprintf(szdpcv[j]		        , "Optional DataPt %d Custom Value",j+1);
             szDataPt_Map2AssetID[j]= (char*) malloc (100); sprintf(szDataPt_Map2AssetID[j] , "Optional DataPt %d Mapping AssetID",j+1);
             szDataPt_Map2AspectSet[j]= (char*) malloc (100); sprintf(szDataPt_Map2AspectSet[j] , "Optional DataPt %d Mapping AspectSet",j+1);
             szDataPt_Map2AspectVar[j]= (char*) malloc (100); sprintf(szDataPt_Map2AspectVar[j] , "Optional DataPt %d Mapping AspectVar",j+1);
             szDataPt_MapStatus[j]  = (char*) malloc (100); sprintf(szDataPt_MapStatus[j] , "ToBeDone DataPt %d Mapping Status",j+1);
	 		 szMinDeltaValue[j]		= (char*) malloc (10);  sprintf(szMinDeltaValue[j]		, "0%%");
	 		 fMinDeltaValue[j]		= 0;
	 		 nMaxDeltaTime[j]		= 0;
	 		 nDataPtInCfg[j]		= 0;
	 		 nDataPtKeepMapping[j]  = 1;
	 	}

		// Events

		szEvent_Source 		= (char **) malloc(MAXEV * sizeof(char*));
		szEvent_Description	= (char **) malloc(MAXEV * sizeof(char*));
		//szEvent_CID			= (char **) malloc(MAXEV * sizeof(char*));

		for (j = 0; j < MAXEV; j++)
	 	{
			szEvent_Source[j]		= (char*) malloc (100); 	sprintf(szEvent_Source[j]	, "ToBeDone Event Source %d",j+1);
			szEvent_Description[j]	= (char*) malloc (100); 	sprintf(szEvent_Description[j]	, "ToBeDone Event Description %d",j+1);
			//szEvent_CID[j]			= (char*) malloc (30);  sprintf(szEvent_CID[j]			, "ToBeDone Event CID %d",j+1);
			Event_Severity[j] 		= MCL_EVENT_SEVERITY_INFORMATION;
	 	}



		// Hier entsteht 1 Datenquellen und mit 1 Datenpunkt bei nicht vorhandenenr conf Datei -> als Beispiel ...
		nDataSrc_Count	= 1;
		nDataSrc_DataPointsCount[0] = 1;
		nEvent_Count = 1;



	 	bWrite = 0;


		if ( config_read_file(&cfg, config_file))
		{
			CfgRoot = config_root_setting(&cfg);
			sprintf(szPrintOut,"Reading Agent configuration File: %s\n",config_file);agentdprint (szPrintOut);
		}
		else
		{
			bWrite++ ;
			config_init(&cfg);
			CfgRoot = config_root_setting(&cfg);
			sprintf(szPrintOut,"Creating Agent configuration File: %s\n",config_file);agentdprint (szPrintOut);
		}
		// MCL Agent settings

		GetSetConfigString("Agent_DebugFile",			&szAgentDebugFile,			CfgRoot, &bWrite, 0);
				if (szAgentDebugFile != NULL) unlink (szAgentDebugFile);


		if (NULL == (CfgGroup = config_setting_get_member(CfgRoot,"MCL-Config")))
		  {CfgGroup = config_setting_add(CfgRoot, "MCL-Config", CONFIG_TYPE_GROUP); bWrite++ ;}

		GetSetConfigString("MindSphereBaseUrl", 		&szCfg_MdSphUrl, 			CfgGroup, &bWrite, 0);
		GetSetConfigInt   ("Port", 						&port, 						CfgGroup, &bWrite);
		GetSetConfigString("MindSphereCertificate",		&szCert, 					CfgGroup, &bWrite, 0);
		GetSetConfigString("DataLakeCertificate",		&szData_Lake_Certificate, 	CfgGroup, &bWrite, 0);
		GetSetConfigInt   ("Security_Profile",			&security_profile, 			CfgGroup, &bWrite);
		GetSetConfigString("Proxy_Hostname",			&szproxy_hostname,			CfgGroup, &bWrite, 0);
		GetSetConfigInt   ("Proxy_Port",				&proxy_port, 				CfgGroup, &bWrite);
		GetSetConfigInt   ("Proxy_Type",				&proxy_type, 				CfgGroup, &bWrite);
		GetSetConfigString("Proxy_Username",			&szproxy_username,			CfgGroup, &bWrite, 0);
		GetSetConfigString("Proxy_Password",			&szproxy_password,			CfgGroup, &bWrite, 0);
		GetSetConfigString("Proxy_Domain",				&szproxy_domain,			CfgGroup, &bWrite, 0);
		GetSetConfigInt   ("Max_Http_Payload_Size",		&max_http_payload_size, 	CfgGroup, &bWrite);
		GetSetConfigInt   ("Http_Request_Timeout",		&http_request_timeout, 		CfgGroup, &bWrite);
		GetSetConfigInt   ("TimeStamp_Precision",		&TimeStamp_Precision, 		CfgGroup, &bWrite);
		GetSetConfigString("User_Agent",				&szuser_agent,				CfgGroup, &bWrite, 1);
		GetSetConfigString("Initial_Access_Token",		&szinitial_access_token,	CfgGroup, &bWrite, 0);
		GetSetConfigString("Tenant",					&sztenant,					CfgGroup, &bWrite, 0);
		GetSetConfigInt   ("Agent_SleepTime",			&nAgentSleepTime, 			CfgGroup, &bWrite);
		GetSetConfigInt   ("Agent_StartDelay",			&nAgentStartDelay, 			CfgGroup, &bWrite);
		GetSetConfigInt   ("MCL_DebugLevel",			&nDebugLevel,	 			CfgGroup, &bWrite);
		GetSetConfigString("MCL_DebugFile",				&szMclDebugFile,			CfgGroup, &bWrite, 0);
		mcl_log_util_set_output_level(nDebugLevel);

		// Data Model
		if (NULL == (CfgGroup = config_setting_get_member(CfgRoot,"Data-Model")))
		{CfgGroup = config_setting_add(CfgRoot, "Data-Model", CONFIG_TYPE_GROUP); bWrite++;}


		GetSetConfigString("DataConfig_ID",				&szDataConfig_ID,			CfgGroup, &bWrite, 0);
		GetSetConfigString("TimeSeriesVersion",			&szTimeSeriesVersion,		CfgGroup, &bWrite, 0);
		GetSetConfigString("DataSourceVersion",			&szDataSourceVersion,		CfgGroup, &bWrite, 0);
		GetSetConfigString("DataInputFile",				&szDataInputFile,			CfgGroup, &bWrite, 0);
		GetSetConfigInt   ("DataSource-Count", 			&nDataSrc_Count,			CfgGroup, &bWrite);

		GetSetConfigString("EventInputFile",			&szEventInputFile,			CfgGroup, &bWrite, 0);
		GetSetConfigInt   ("Event-Count", 				&nEvent_Count,	    		CfgGroup, &bWrite);
		GetSetConfigString("FileUploadDirectory",		&szUpLoadDir,				CfgGroup, &bWrite, 0);
		GetSetConfigString("DataLakeFileUploadDirectory",&szUpLoadDirDL,	       	CfgGroup, &bWrite, 0);
		GetSetConfigString("DataLakeSubTenant",         &szSubTenantDL, 	       	CfgGroup, &bWrite, 0);
        GetSetConfigString("DataLakeUploadUrl",         &szUpLoadUrlDL, 	       	CfgGroup, &bWrite, 0);


		if (nDataSrc_Count > MAXDS)
		{
		  sprintf (szPrintOut,"CfgErr: DataSource-Count(%d) out of Range (1 - %d)  -> additional Sources are ignored", nDataSrc_Count, MAXDS);agentdprint(szPrintOut);
		  nDataSrc_Count = MAXDS;
		  err++;
		}

		if (nEvent_Count > MAXEV)
		{
		  sprintf (szPrintOut,"CfgErr: Event-Count(%d) out of Range (1 - %d)  -> additional Events are ignored", nEvent_Count, MAXEV);agentdprint(szPrintOut);
		  nEvent_Count = MAXEV;
		  err++;
		}

		// Datasources
		char * szNo = malloc(256);
		char *szVal = malloc(256);
		nDataPoints = 0;

		for (i = 0; ((i < nDataSrc_Count) && (nDataPoints < MAXDP )) ; i++)
		{
			  sprintf(szNo,"DataSource%d-", i+1);
			  sprintf(szVal,"%sDataPointsCount",szNo);	GetSetConfigInt   (szVal,		&nDataSrc_DataPointsCount[i],		CfgGroup, &bWrite);
			  sprintf(szVal,"%sDescription",szNo);		GetSetConfigString(szVal,		&szDataSrc_Description[i],			CfgGroup, &bWrite, 1);
			  sprintf(szVal,"%sName",szNo);				GetSetConfigString(szVal,		&szDataSrc_Name[i],					CfgGroup, &bWrite, 1);
			  sprintf(szVal,"%sCustomField",szNo);		GetSetConfigString(szVal,		&szdscf[i],		        			CfgGroup, &bWrite, 1);
			  sprintf(szVal,"%sCustomValue",szNo);		GetSetConfigString(szVal,		&szdscv[i],		        			CfgGroup, &bWrite, 1);


			  // Datapoints per Datasource
			  int j,k;
			  for (j = 0, k = nDataPoints; j < nDataSrc_DataPointsCount[i]; j++, k++)
			  {
					nDataPoints++;
					if (nDataPoints >= MAXDP)
					{
						  sprintf (szPrintOut,"CfgErr: DataPoints-Count(%d) out of Range (1 - %d)  -> additional Points are ignored", nDataPoints, MAXDP);agentdprint(szPrintOut);
						  nDataSrc_Count = MAXDS;
						  err++;
						  break;
					}

					nDataPtInCfg[nDataPoints-1] = (i+1)*1000 + (j+1);

					sprintf(szNo,"DataPoint%d-", (i+1)*1000 + (j+1));
					sprintf(szVal,"%sName",szNo);			GetSetConfigString(szVal,			&szDataPt_Name[k],				CfgGroup, &bWrite, 1);
					sprintf(szVal,"%sDataType",szNo);		GetSetConfigString(szVal,			&szDataPt_DataType[k],			CfgGroup, &bWrite, 1);
					sprintf(szVal,"%sUnit",szNo);			GetSetConfigString(szVal,			&szDataPt_Unit[k],				CfgGroup, &bWrite, 1);
					sprintf(szVal,"%sDescription",szNo);	GetSetConfigString(szVal,			&szDataPt_Description[k],		CfgGroup, &bWrite, 1);
					sprintf(szVal,"%sMinDeltaVal",szNo);	GetSetConfigString(szVal,			&szMinDeltaValue[k],			CfgGroup, &bWrite, 0);
					sprintf(szVal,"%sMaxDeltaTime",szNo);	GetSetConfigInt   (szVal,			&nMaxDeltaTime[k],				CfgGroup, &bWrite);
					sprintf(szVal,"%sCustomField",szNo);	GetSetConfigString(szVal,		    &szdpcf[k],		       			CfgGroup, &bWrite, 1);
                    sprintf(szVal,"%sCustomValue",szNo);	GetSetConfigString(szVal,		    &szdpcv[k],		       			CfgGroup, &bWrite, 1);
                    sprintf(szVal,"%sMap2AssetID",szNo);	GetSetConfigString(szVal,		    &szDataPt_Map2AssetID[k],	    CfgGroup, &bWrite, 1);
                    sprintf(szVal,"%sMap2AspectSet",szNo);	GetSetConfigString(szVal,		    &szDataPt_Map2AspectSet[k],	    CfgGroup, &bWrite, 1);
                    sprintf(szVal,"%sMap2AspectVar",szNo);	GetSetConfigString(szVal,		    &szDataPt_Map2AspectVar[k],	    CfgGroup, &bWrite, 1);
                    sprintf(szVal,"%sKeepMapping",szNo);	GetSetConfigInt   (szVal,			&nDataPtKeepMapping[k],		    CfgGroup, &bWrite);
                    sprintf(szVal,"%sMapStatus",szNo);	    GetSetConfigString(szVal,		    &szDataPt_MapStatus[k],	        CfgGroup, &bWrite, 0);


					sprintf(szVal,"%sID",szNo);				GetSetConfigString(szVal,			&szDataPt_ID[k],				CfgGroup, &bWrite, 1);
					if (strncmp(szDataPt_ID[k], "ToBeDone",8) == 0) // falls noch keine UUID vorhanden , eine generieren...
					{
						mcl_random_generate_guid(&szDataPt_ID[k]);
						CfgMember = config_setting_get_member(CfgGroup,szVal);
						config_setting_set_string(CfgMember, szDataPt_ID[k]);
					}
			  }

		}

		// Events
		for (i = 0; i < nEvent_Count  ; i++)
		{
			sprintf(szNo,"Event%04d-", i+1);
			sprintf(szVal,"%sSource",szNo);				GetSetConfigString(szVal,		&szEvent_Source[i],					CfgGroup, &bWrite, 1);
			sprintf(szVal,"%sDescription",szNo);		GetSetConfigString(szVal,		&szEvent_Description[i],			CfgGroup, &bWrite, 1);
			sprintf(szVal,"%sSeverity",szNo);			GetSetConfigInt   (szVal,		&Event_Severity[i],					CfgGroup, &bWrite);
		/*
			sprintf(szVal,"%sCID",szNo);				GetSetConfigString(szVal,		&szEvent_CID[i],	    			CfgGroup, &bWrite, 1);
			if (strncmp(szEvent_CID[i], "ToBeDone",8) == 0) // falls noch keine UUID vorhanden , eine generieren...
			{
				mcl_random_generate_guid(&szEvent_CID[i]);
				CfgMember = config_setting_get_member(CfgGroup,szVal);
				config_setting_set_string(CfgMember, szEvent_CID[i]);
			}
		*/
		}


		//Plausi Pr�fungen

       // if (strncmp (szSubTenantDL,"Optional", 8) == 0)
         //   szSubTenantDL[0] = 0;

        //if (strncmp (szUpLoadUrlDL,"Optional", 8) == 0)
		  //  szUpLoadUrlDL[0] = 0;


		// DS auf Projektierung prüfen
		for (i = 0 ;  i < nDataSrc_Count;  i++)
		{
			if (strncmp (szDataSrc_Name[i],"ToBeDone", 8) == 0)
			{
				sprintf(szPrintOut,"CfgErr: DataSource: %03d -> Please edit: (%s)\n",i+1,  szDataSrc_Name[i]); agentdprint (szPrintOut); err++;
			}
			if (strncmp (szDataSrc_Description[i],"ToBeDone", 8) == 0)
			{
				sprintf(szPrintOut,"CfgErr: DataSource: %03d -> Please edit: (%s)\n",i+1,  szDataSrc_Description[i]); agentdprint (szPrintOut); err++;
			}
			/*
			if (strncmp (szdscf[i],"Optional", 8) == 0)
			{
                szdscf[i][0] = 0;
			}
			if (strncmp (szdscv[i],"Optional", 8) == 0)
			{
                szdscv[i][0] = 0;
			}*/

		}

		// DP Id auf Doppelte ID's prüfen
		for (i = 0 ;  i < nDataPoints;  i++)
		{
			for (ii = i+1; ii < nDataPoints;  ii++)
			{
				if (!strcmp (szDataPt_ID[i],szDataPt_ID[ii]))
				{
					sprintf(szPrintOut,"CfgErr: Datapoints: %03d / %03d  -> Duplicate ID: (%s)\n",i+1, ii+1, szDataPt_ID[ii]); agentdprint (szPrintOut); err++;
				}
			}
		}

		// DP auf Projektierung prüfen
		for (i = 0 ;  i < nDataPoints;  i++)
		{
			if (strncmp (szDataPt_Name[i],"ToBeDone", 8) == 0)
			{
				sprintf(szPrintOut,"CfgErr: Datapoint: %03d -> Please edit: (%s)\n",i+1,  szDataPt_Name[i]); agentdprint (szPrintOut); err++;
			}

			if (strncmp (szDataPt_DataType[i],"ToBeDone", 8) == 0)
			{
				sprintf(szPrintOut,"CfgErr: Datapoint: %03d -> Please edit: (%s)\n",i+1,  szDataPt_DataType[i]); agentdprint (szPrintOut); err++;
			}
			else
			{
				int found = 0;
				char szTyp[30];
				strcpy(szTyp, szDataPt_DataType[i]);
				strlwr(szTyp);
				if 	(!strcmp (szTyp,"boolean")) 	found++;
				if	(!strcmp (szTyp,"int")) 		found++;
				if	(!strcmp (szTyp,"long"))    	found++;
				if	(!strcmp (szTyp,"double"))		found++;
				if	(!strcmp (szTyp,"string")) 		found++;
				if	(!strcmp (szTyp,"big_string")) 	found++;
				if	(!strcmp (szTyp,"timestamp"))	found++;

				if (!found)
				{
					sprintf(szPrintOut,"CfgErr: Datapoint: %03d -> Unknown Data Type: (%s)\n",i+1,  szDataPt_DataType[i]); agentdprint (szPrintOut); err++;
				}
			}

			if (strncmp (szDataPt_Unit[i],"ToBeDone", 8) == 0)
			{
				sprintf(szPrintOut,"CfgErr: Datapoint: %03d -> Please edit: (%s)\n",i+1,  szDataPt_Unit[i]); agentdprint (szPrintOut); err++;
			}

			if (strncmp (szDataPt_Description[i],"ToBeDone", 8) == 0)
			{
				sprintf(szPrintOut,"CfgErr: Datapoint: %03d -> Please edit: (%s)\n",i+1,  szDataPt_Description[i]); agentdprint (szPrintOut); err++;
			}
			/*
			if (strncmp (szdpcf[i],"Optional", 8) == 0)
			{
                szdpcf[i][0] = 0;
			}
			if (strncmp (szdpcv[i],"Optional", 8) == 0)
			{
                szdpcv[i][0] = 0;
			}
			if (strncmp (szDataPt_Map2AssetID[i],"Optional", 8) == 0)
			{
                szDataPt_Map2AssetID[i][0] = 0;
			}
			if (strncmp (szDataPt_Map2AspectSet[i],"Optional", 8) == 0)
			{
                szDataPt_Map2AspectSet[i][0] = 0;
			}
			if (strncmp (szDataPt_Map2AspectVar[i],"Optional", 8) == 0)
			{
                szDataPt_Map2AspectVar[i][0] = 0;
			} */
		}

		// Events auf Projektierung prüfen
		for (i = 0 ;  i < nEvent_Count;  i++)
		{
			if (strncmp (szEvent_Source[i],"ToBeDone", 8) == 0)
			{
				sprintf(szPrintOut,"CfgErr: Event: %04d -> Please edit: (%s)\n",i+1,  szEvent_Source[i]); agentdprint (szPrintOut); err++;
			}

		/*	if (strncmp (szEvent_CID[i],"ToBeDone", 8) == 0)
			{
				sprintf(szPrintOut,"CfgErr: Event: %04d -> Please edit: (%s)\n",i+1,  szEvent_CID[i]); agentdprint (szPrintOut); err++;
			} */

			if (strncmp (szEvent_Description[i],"ToBeDone", 8) == 0)
			{
				sprintf(szPrintOut,"CfgErr: Event: %04d -> Please edit: (%s)\n",i+1,  szEvent_Description[i]); agentdprint (szPrintOut); err++;
			}
		}

		if (access(szUpLoadDir,F_OK))
		{
			if (mkdir(szUpLoadDir,0777))
			{
				sprintf(szPrintOut,"CfgErr: FileUploadDirectory: %s not accessible / not creatable -> Please edit and/or check\n", szUpLoadDir); agentdprint (szPrintOut); err++;
			}
		}

		if (access(szUpLoadDirDL,F_OK))
		{
			if (mkdir(szUpLoadDirDL,0777))
			{
				sprintf(szPrintOut,"CfgErr: FileUploadDirectory DataLake: %s not accessible / not creatable -> Please edit and/or check\n", szUpLoadDirDL); agentdprint (szPrintOut); err++;
			}
		}

		if ((TimeStamp_Precision < 0)|| (TimeStamp_Precision > 3))
		{
		  sprintf (szPrintOut,"CfgErr: TimeStamp_Precision: (%d) out of Range: (0-3) -> default = (0)", TimeStamp_Precision);agentdprint(szPrintOut);
		  TimeStamp_Precision = 0; err++;
		}


		// Write out the new configuration.
		if (bWrite)
		{
			if (! config_write_file(&cfg, config_file))
				{sprintf(szPrintOut,"Error while writing config file.\n");agentdprint(szPrintOut);}
			else
				{sprintf(szPrintOut,"Configuration File successfully written: %s\n",config_file);agentdprint(szPrintOut);}
		}
		else {sprintf(szPrintOut,"Reading Configuration finished: %s\n",config_file);agentdprint(szPrintOut);}

		if (err)
		{
			agentdprint("Configuration errors found  -> configuration file is faulty / incomplete\n\nAgent exits\n");
			cleanup(); exit (-1);
		}

		chknfree(szNo);
		chknfree(szVal);


 }

static long GetTimeInMs (char* szDT)	// Zeit in Tages MilliSekunden umrechnen
{
	// 2018-04-05T22:54:16.347Z
	// 012345678901234567890123
	// 0         1         2
	//char szT[100];
	long  h, m, s, ms, tms;

	//strcpy (szT,szDT);
	//szT[13] = 0;szT[16] = 0; szT[19] = 0; szT[23] = 0;

	h = atol (&(szDT[11]));
	m = atol (&(szDT[14]));
	s = atoi (&(szDT[17]));
	ms= atol (&(szDT[20]));

	tms = ((h*3600 + m*60 + s) * 1000) + ms;
	return (tms);
}

// Map each Datpoint
void manage_data_mapping(mcl_connectivity_t *connectivity)
{
  mcl_error_t code;

  int i;
  bWrite = 0;


  {sprintf(szPrintOut,"Mapping of %d Datapoints in progress...\n",nDataPoints);agentdprint(szPrintOut);}

  code = MCL_OK;

    for (i = 0 ;  i < nDataPoints;  i++)
    {
        mcl_mapping_t *mapping = NULL;

        if
        (
           (0 == (strncmp( szDataPt_Map2AspectSet[i], "Optional",8))) ||
           (0 == (strncmp( szDataPt_Map2AspectVar[i], "Optional",8))) ||
           (0 == (strncmp( szDataPt_Map2AssetID[i],   "Optional",8)))
        )   {
                {sprintf(szPrintOut,"Skipping DataPoint %d %s - not configured\n",nDataPtInCfg[i], szDataPt_Name[i]); agentdprint(szPrintOut);}
                continue;
            }

        if (nNewDataModelCreated == 0 ) // Neues Datenmodel übertragen bedeutet auf alle Fälle neues Mapping
        {
            if ( 0 == (strncmp( szDataPt_MapStatus[i], "MCL_OK",6))) // das vorhandene Datenmodell , was schon erfolgreich gemappt ist , überspringen
            {
                    {sprintf(szPrintOut,"Skipping DataPoint %d %s - already mapped (cfg status: MCL_OK)\n",nDataPtInCfg[i], szDataPt_Name[i]); agentdprint(szPrintOut);}
                    continue;
            }
        }

        // Initialize mapping.
        code = mcl_mapping_initialize(&mapping);
        bWrite++;

        // Set "Data Point ID".
        if (MCL_OK == code)
            code = mcl_mapping_set_parameter(mapping, MCL_MAPPING_PARAMETER_DATA_POINT_ID,szDataPt_ID[i] );

        // Set "Property Set Name".
        if (MCL_OK == code)
            code = mcl_mapping_set_parameter(mapping, MCL_MAPPING_PARAMETER_PROPERTY_SET_NAME, szDataPt_Map2AspectSet[i]);

        // Set "Propert Name".
        if (MCL_OK == code)
            code = mcl_mapping_set_parameter(mapping, MCL_MAPPING_PARAMETER_PROPERTY_NAME,szDataPt_Map2AspectVar[i]); // property_names[index]);

        // Set "Entity ID".
       if (MCL_OK == code)
            code = mcl_mapping_set_parameter(mapping, MCL_MAPPING_PARAMETER_ENTITY_ID, szDataPt_Map2AssetID[i]);

       if (MCL_OK == code)
            code = mcl_mapping_set_parameter(mapping, MCL_MAPPING_PARAMETER_KEEP_MAPPING, &(nDataPtKeepMapping[i])); // property_names[index]);

        // Create mapping.
        if (MCL_OK == code)
            code = mcl_connectivity_create_mapping(connectivity, mapping);

        mcl_mapping_destroy(&mapping);


        {sprintf(szPrintOut,"Mapping DataPoint %d %s %s\n",nDataPtInCfg[i], szDataPt_Name[i],MCL_CORE_CODE_TO_STRING(code));agentdprint(szPrintOut);}

        char szMember[100];
        sprintf(szMember, "DataPoint%d-MapStatus",nDataPtInCfg[i]);


        CfgGroup = config_setting_get_member(CfgRoot,"Data-Model");
        CfgMember = config_setting_get_member(CfgGroup,szMember);
        config_setting_set_string(CfgMember, MCL_CORE_CODE_TO_STRING(code));

    }
    if (bWrite)
        config_write_file(&cfg, config_file);

    agentdprint("Mapping Datapoints finished...\n");
}


mcl_size_t data_lake_file_callback(char *buffer, mcl_size_t size, mcl_size_t count, void *file_descriptor)
{
    mcl_size_t read_size = 0;

    mcl_file_util_fread(buffer, size, count, file_descriptor, &read_size);

    return read_size;
}

mcl_error_t get_access_token(mcl_core_t *core)
{
    mcl_error_t code;

    code = mcl_core_get_access_token(core);
    sprintf (szPrintOut,"Getting access token: %s.\n", MCL_CORE_CODE_TO_STRING(code));agentdprint(szPrintOut);


    if (MCL_BAD_REQUEST == code)
    {
        // Key rotation is needed.
        code = mcl_core_rotate_key(core);
        sprintf (szPrintOut,"Rotating key: %s.\n", MCL_CORE_CODE_TO_STRING(code));agentdprint(szPrintOut);


        if (MCL_OK == code)
        {
            code = mcl_core_get_access_token(core);
            sprintf (szPrintOut,"Getting access token after key rotation: %s.\n", MCL_CORE_CODE_TO_STRING(code));agentdprint(szPrintOut);
        }
    }

    return code;
}




char   szTmp[MAXDP][100];  // enthält Temp und Zeitstempel
double dOldVal [MAXDP], dNewVal  [MAXDP];
long   lOldTime[MAXDP], lNewTime [MAXDP];

void manage_data_lake_file_upload(mcl_core_t *core)
{
	E_MCL_CORE_RETURN_CODE code;
	mcl_data_lake_configuration_t *data_lake_configuration = NULL;
    mcl_data_lake_t *data_lake = NULL;

    char szFullAdr [MAXNAMLEN];
	char szFileName [MAXNAMLEN];
	DIR *pDir;
	struct dirent *pDirEntry;
	char *pExt;
	char *pEmpty = "";


	// check if folder is not empty
	pDir = opendir(szUpLoadDirDL);
	if (pDir != NULL)
	{
		while ((pDirEntry = readdir(pDir)) != NULL )
		{
			if (strcmp (pDirEntry->d_name,".")  == 0) continue;
			if (strcmp (pDirEntry->d_name,"..") == 0) continue;
			if ((*pDirEntry->d_name) == '.') continue;

			sprintf(szFileName,"%s%s", szUpLoadDirDL,pDirEntry-> d_name);
			if (access(szFileName,F_OK)) continue;

			pExt = strrchr(pDirEntry->d_name,'.');
			if (pExt != NULL)
				pExt++;
			else
				pExt = pEmpty;

            // Initialize data lake configuration.


            code = mcl_data_lake_configuration_initialize(&data_lake_configuration);
            //sprintf (szPrintOut, "Initializing data lake configuration: %s.\n", MCL_DATA_LAKE_CODE_TO_STRING(code));agentdprint(szPrintOut);


            // Initialize data lake.
            if (MCL_OK == code)
            {
                mcl_data_lake_configuration_set_parameter(data_lake_configuration, MCL_DATA_LAKE_CONFIGURATION_PARAMETER_CORE, core);
                mcl_data_lake_configuration_set_parameter(data_lake_configuration, MCL_DATA_LAKE_CONFIGURATION_PARAMETER_CERTIFICATE, szData_Lake_Certificate);

                // Initialize mcl data lake.
                code = mcl_data_lake_initialize(data_lake_configuration, &data_lake);
              //  sprintf (szPrintOut, "Initializing mcl data lake: %s.\n", MCL_DATA_LAKE_CODE_TO_STRING(code));agentdprint(szPrintOut);
            }

			if (MCL_OK == code)
            {
                mcl_data_lake_object_t *data_lake_object = NULL;
                void *file_descriptor = NULL;


                // Initialize data lake object.
                code = mcl_data_lake_object_initialize(&data_lake_object);
                //printf("Initializing mcl data lake object: %s.\n", MCL_DATA_LAKE_CODE_TO_STRING(code));

                if (MCL_OK == code)
                {
                    // Set path to generate upload url.
                    int i;
                    i = strlen (szUpLoadUrlDL);

                    if (i > 0)
                    {
                        if  (szUpLoadUrlDL[i-1] != '/') // szUpLoadURLDir ist gross genug, kein Risiko
                        {
                            szUpLoadUrlDL[i] = '/';
                            szUpLoadUrlDL[i+1] = 0;
                        }
                    }


                    sprintf(szFullAdr,"%s%s",szUpLoadUrlDL,pDirEntry-> d_name );
                    code = mcl_data_lake_object_set_parameter(data_lake_object, MCL_DATA_LAKE_OBJECT_PARAMETER_PATH, szFullAdr);
                    //printf("Setting path of mcl data lake object: %s.\n", MCL_DATA_LAKE_CODE_TO_STRING(code));
                }

                // Generate upload url.
                if (MCL_OK == code)
                {
                    if (strlen(szSubTenantDL) == 0)
                    {
                        code = mcl_data_lake_generate_upload_url(data_lake, data_lake_object);
                    }
                    else
                    {
                        code = mcl_data_lake_generate_upload_url_for_subtenant(data_lake, data_lake_object, szSubTenantDL);
                    }

                    //printf("Generating upload url: %s.\n", MCL_DATA_LAKE_CODE_TO_STRING(code));
                }

                if (MCL_OK == code)
                {

                    code = mcl_file_util_fopen(szFileName, "rb", &file_descriptor);
                    //printf("Opening the file which will be uploaded: %s.\n", MCL_CORE_CODE_TO_STRING(code));
                }

                // Set parameters of data lake object.
                if (MCL_OK == code)
                {
                    mcl_size_t file_size = mcl_file_util_get_file_size(file_descriptor);
                    mcl_data_lake_object_set_parameter(data_lake_object, MCL_DATA_LAKE_OBJECT_PARAMETER_SIZE, &file_size);
                    mcl_data_lake_object_set_parameter(data_lake_object, MCL_DATA_LAKE_OBJECT_PARAMETER_USER_CONTEXT, file_descriptor);
                    mcl_data_lake_object_set_parameter(data_lake_object, MCL_DATA_LAKE_OBJECT_PARAMETER_UPLOAD_CALLBACK, &data_lake_file_callback);

                    code = mcl_data_lake_upload(data_lake, data_lake_object);
                    sprintf (szPrintOut, "Uploading file to data lake:  %s -> %s.\n", szFullAdr, MCL_DATA_LAKE_CODE_TO_STRING(code));agentdprint(szPrintOut);
                }

                if (MCL_OK != code)
                   { sprintf (szPrintOut, "Failure Uploading file to data lake:  %s -> %s.\n", pDirEntry-> d_name, MCL_DATA_LAKE_CODE_TO_STRING(code));agentdprint(szPrintOut);}

                if((MCL_FORBIDDEN == code) || (MCL_UNAUTHORIZED == code))
                    get_access_token(core);
                else
                    unlink(szFileName); // Im Gutfall wird nach dem Upload

                mcl_file_util_fclose(file_descriptor);
                mcl_data_lake_object_destroy(&data_lake_object);
            }

            // Clean up.
            mcl_data_lake_destroy(&data_lake);
            mcl_data_lake_configuration_destroy(&data_lake_configuration);
		// At the end all resources should be freed (e.g store, connectivity etc.).
		} // while pDirEntry
		closedir(pDir);
	} // if pDir
}


void manage_file_upload(mcl_connectivity_t *connectivity)
{
	E_MCL_CORE_RETURN_CODE code;


	char szFileName [MAXNAMLEN];
	DIR *pDir;
	struct dirent *pDirEntry;
	char *pExt;
	char *pEmpty = "";


	// check if folder is not empty


	pDir = opendir(szUpLoadDir);
	if (pDir != NULL)
	{
		while ((pDirEntry = readdir(pDir)) != NULL )
		{
			if (strcmp (pDirEntry->d_name,".")  == 0) continue;
			if (strcmp (pDirEntry->d_name,"..") == 0) continue;
			if ((*pDirEntry->d_name) == '.') continue;

			sprintf(szFileName,"%s%s", szUpLoadDir,pDirEntry-> d_name);
			if (access(szFileName,F_OK)) continue;

			pExt = strrchr(pDirEntry->d_name,'.');
			if (pExt != NULL)
				pExt++;
			else
				pExt = pEmpty;

			// For any exchange operation a store must be created.
			mcl_store_t *store = NULL;
			code = mcl_store_initialize( &store);

			// In order to exchange file it must be added to store.
			mcl_file_t *file = NULL;

			// Initialize file for exchange.
			if (MCL_OK == code)
			{
			   code = mcl_file_initialize(MCL_FILE_VERSION_1_0, &file);
			}

			// Set file name.
			if (MCL_OK == code)
			{
				code = mcl_file_set_parameter(file, MCL_FILE_PARAMETER_TYPE, pExt);
			}

			// Set file path.
			if (MCL_OK == code)
			{
				code = mcl_file_set_parameter(file, MCL_FILE_PARAMETER_REMOTE_NAME, pDirEntry-> d_name);
			}

			//  Set file type.
			if (MCL_OK == code)
			{
				code = mcl_file_set_parameter(file, MCL_FILE_PARAMETER_LOCAL_PATH, szFileName);
			}

			//code = mcl_store_new_file(store, "1.0", szFileName, pDirEntry->d_name, pExt, NULL, &file);

			// If setting event details is successful, exchange the store.
			int nSndCtr = 3;

			while( nSndCtr > 0)
			{
				struct timespec tstart={0,0}, tend={0,0};
				double tt;

				nSndCtr--;
				clock_gettime(CLOCK_MONOTONIC, &tstart);
				// code = mcl_communication_exchange(connectivity, store, NULL);
				code = mcl_connectivity_exchange(connectivity, (void *)file);
				clock_gettime(CLOCK_MONOTONIC, &tend);

				lUploadsSent++;

				if (code == MCL_OK)
				{

					tt = ((double)tend.tv_sec + 1.0e-9*tend.tv_nsec) - ((double)tstart.tv_sec + 1.0e-9*tstart.tv_nsec);
					lUploadsSentOk++;
					sprintf(szPrintOut,"File Upload: %s\n",szFileName); agentdprint(szPrintOut);
					sprintf(szPrintOut,"File Data Store (%ld) successfully uploaded...in %f s / ServerErrors since Start: %ld\n",lUploadsSentOk, tt, lServerError); agentdprint(szPrintOut);
					unlink(szFileName);
					nSleepPts = 0;     // sleep message ggf. ausgeben
					break; // erfolgreich gesendet , raus ...
				}
				// If unauthorized error received, call rotate key to generate a new authentication key.
				else if (MCL_UNAUTHORIZED == code)
				{
					lRotateKeys++;
					nSndCtr--;
					agentdprint("Key rotation is being done : ");
					//code = mcl_communication_rotate_key(connectivity); // danach timeseries noch mal senden ...
					code = mcl_core_rotate_key(core);
					sprintf(szPrintOut, "%s.\n", MCL_CORE_CODE_TO_STRING(code)); agentdprint(szPrintOut);
					code = mcl_core_get_access_token(core);
				}
				else
				{
					lServerError++;
					sprintf(szPrintOut,"Upload of File Store (%ld) failed: %s --> sleeping %ds \n",lUploadsSent, MCL_CORE_CODE_TO_STRING(code),nAgentSleepTime);   agentdprint(szPrintOut);
					sleep(nAgentSleepTime);


					if ( nSndCtr == 0)  // nSndCtr (3) Versuche
					{
						mcl_store_destroy(&store);
						mcl_connectivity_destroy(&connectivity);
						exit(-1) ; // fataler Fehler -> Agent beenden
					}
				}
			}// while (sndctr)
			mcl_store_destroy(&store); // At the end all resources should be freed (e.g store, connectivity etc.).
		} // while pDirEntry
		closedir(pDir);
	} // if pDir
}

void manage_events(mcl_connectivity_t *connectivity)
{

	// Event input file checken
	FILE *fpT;

	fpT = fopen(szEventInputFile, "r");

	if (fpT == NULL) // kein Event Input  File da
		return ;

	// Alle Events auslesen



	E_MCL_CORE_RETURN_CODE code;
	//mcl_store_t *store = NULL;
	//mcl_event_t *event = NULL;
	mcl_event_t *my_event = NULL;
	mcl_json_t *event_details = NULL;
	char szTmpAct[100];
	char szInp[100];
	char *pEV, *pT, *pC, *pA;
	char* pWorktodo = NULL;
	int offset;
	//int nLinesCtr = 0;
	//int nInputLinesCtr= 0;
	while ((pWorktodo = fgets (szTmpAct,99,fpT)) != NULL )//| nWork) // ganze Zeile holen
	{

		// Split the Input String
		pEV = szTmpAct;							// Event ID
		pT = strchr(pEV,'|'); 	*pT = 0; pT++;	// Timestamp
		pC = strchr(pT, '|'); 	*pC = 0; pC++;	// Code
		pA = strchr(pC, '|'); 	*pA = 0; pA++;	// Ack

		*(pA+1) = 0;  // \n l�schen

		if ( pEV == NULL || pT == NULL || pC == NULL || pA == NULL)
		{
			return;	// error message
		}
		pT[10]= 'T'; 	pT[23]= 'Z'; 	pT[24]= 0; // f�r Mindsphere Zeit Format anpassen

		offset = (atoi(pEV)) - 1;

		// Plausi pr�fung
		if ((offset < 0) || (offset > nEvent_Count))
			continue;


		// Initializing an event.
		code = mcl_event_initialize(MCL_EVENT_VERSION_1_0, &my_event);


		// Setting severity.
		if (MCL_OK == code)
		{
			code = mcl_event_set_parameter(my_event, MCL_EVENT_PARAMETER_SEVERITY, &(Event_Severity[offset]));
		}

		// Setting timestamp.
		if (MCL_OK == code)
		{
			code = mcl_event_set_parameter(my_event, MCL_EVENT_PARAMETER_TIMESTAMP_ISO8601, pT);
		}

		// Setting version.
		if (MCL_OK == code)
		{
			code = mcl_event_set_parameter(my_event, MCL_EVENT_PARAMETER_VERSION, "1.0");
		}

		// Setting type.
		if (MCL_OK == code)
		{
			code = mcl_event_set_parameter(my_event, MCL_EVENT_PARAMETER_TYPE, "MindSphereStandardEvent");
		}



		// set optional field description.

	    char *pStr;
	    pStr = strstr(szEvent_Description[offset] , "%s");

	    if (pStr!= NULL)
	    {
	    	char sztmp1[100];

	    	strcpy(sztmp1,szEvent_Description[offset]);
	    	pStr = strstr(sztmp1 , "%s");

	    	*pStr = 0; // terminate 1st part
	    	sprintf (szInp,"%s%s%s",sztmp1, pC, pStr+2 );

	    }
	    else
	    	sprintf(szInp, "%s %s", pC, szEvent_Description[offset]);  // C n xxxxxxx -> C = Coming/ g = going

	    //code += mcl_event_set_option(event, MCL_EVENT_OPTION_DESCRIPTION,szInp);
	    // Setting description.
		if (MCL_OK == code)
		{
			code = mcl_event_set_parameter(my_event, MCL_EVENT_PARAMETER_DESCRIPTION, szInp);
		}

		code += mcl_json_util_initialize(MCL_JSON_OBJECT, &event_details);
		code += mcl_json_util_add_string(event_details, "source", szEvent_Source[offset]);
		code += mcl_json_util_add_string(event_details, "code", "1");


		if (strstr(strupr(pA) , "A") != NULL)
			code += mcl_json_util_add_bool(event_details, "acknowledged", 1);
		else
			code += mcl_json_util_add_bool(event_details, "acknowledged", 0);

		//code += mcl_event_set_option(event, MCL_EVENT_OPTION_DETAILS, event_details);
		if (MCL_OK == code)
		{
			code = mcl_event_set_parameter(my_event, MCL_EVENT_PARAMETER_DETAILS, event_details);
		}

		// If setting event details is successful, exchange the store.
		int nSndCtr = 3;

		while( nSndCtr > 0)
		{
			struct timespec tstart={0,0}, tend={0,0};
			double tt;

			nSndCtr--;
			clock_gettime(CLOCK_MONOTONIC, &tstart);
			code = mcl_connectivity_exchange(connectivity, my_event);
			clock_gettime(CLOCK_MONOTONIC, &tend);
			lUploadsSent++;

			if (code == MCL_OK)
			{
				tt = ((double)tend.tv_sec + 1.0e-9*tend.tv_nsec) - ((double)tstart.tv_sec + 1.0e-9*tstart.tv_nsec);
				lUploadsSentOk++;
				sprintf(szPrintOut,"Event %04d: %s %s %s %s %s\n",offset+1, pT ,pC, pA, szEvent_Description[offset],szEvent_Source[offset]); agentdprint(szPrintOut);
				sprintf(szPrintOut,"Event Data Store (%ld) successfully uploaded...in %f s / ServerErrors since Start: %ld\n",lUploadsSentOk, tt, lServerError); agentdprint(szPrintOut);
				nSleepPts = 0;     // sleep message ggf. ausgeben
				break; // erfolgreich gesendet , raus ...
			}
			// If unauthorized error received, call rotate key to generate a new authentication key.
			else if (MCL_UNAUTHORIZED == code)
			{
				lRotateKeys++;
				nSndCtr++;
				agentdprint("Key rotation is being done : ");
				//code = mcl_communication_rotate_key(connectivity); // danach timeseries noch mal senden ...
				code = mcl_core_rotate_key(core);
				sprintf(szPrintOut, "%s.\n", MCL_CORE_CODE_TO_STRING(code)); agentdprint(szPrintOut);
				code = mcl_core_get_access_token(core);
			}
			else
			{
				lServerError++;
				sprintf(szPrintOut,"Upload of Event Store (%ld) failed: %s --> sleeping %ds \n",lUploadsSent, MCL_CORE_CODE_TO_STRING(code),nAgentSleepTime);   agentdprint(szPrintOut);
				sleep(nAgentSleepTime);


				if ( nSndCtr == 0)  // nSndCtr (3) Versuche
				{
					//mcl_store_destroy(&store);
					//mcl_communication_destroy(&connectivity);
					mcl_connectivity_destroy(&connectivity);
					fclose (fpT);
					exit(-1) ; // fataler Fehler -> Agent beenden
				}
			}
		}// while (sndctr)
		//mcl_json_util_destroy(&event_details);
		mcl_event_destroy(&my_event);

	}
	unlink (szEventInputFile); // L�schen des abgearbeiteten Event Inputs
	    //mcl_log_util_finalize();
}

static void _setup_configuration(mcl_core_configuration_t *core_configuration)
{
	E_MCL_CORE_RETURN_CODE code;



	    code =  mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_MDSP_HOST, szCfg_MdSphUrl);
	    code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_MDSP_PORT, &port);
	    code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_CERTIFICATE, szCert);

	    if (sizeof(szproxy_hostname) == 0)
            szproxy_hostname = NULL;

	    if (szproxy_hostname != NULL)
            code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_PROXY_HOST, szproxy_hostname);

	    code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_PROXY_PORT, &proxy_port);
	    code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_PROXY_TYPE, &proxy_type);
	    code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_USER_AGENT, szuser_agent);
	    code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_TENANT, sztenant);
	    code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_SECURITY_PROFILE, &security_profile);

	    if (strcmp (szinitial_access_token, "Agent is onboarded"))
            code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_IAT, szinitial_access_token);
        else
           code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_IAT, NULL);

        code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_MAX_HTTP_PAYLOAD_SIZE, &max_http_payload_size);
	    code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_HTTP_REQUEST_TIMEOUT, &http_request_timeout);
	    code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_CREDENTIALS_LOAD_CALLBACK, custom_load_function_shared_secret);
	    code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_CREDENTIALS_SAVE_CALLBACK, custom_save_function_shared_secret);
	    //code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_CRITICAL_SECTION_ENTER_CALLBACK, NULL);//custom_entercs_function);
	    //code += mcl_core_configuration_set_parameter(core_configuration, MCL_CORE_CONFIGURATION_PARAMETER_CRITICAL_SECTION_LEAVE_CALLBACK, NULL);//custom_leavecs_function);

	   // Achtung das muss an der rictigen Stelle gemacht werden , nach der �bergabe / der Config #tobedone
		// F�r Files sind 10MB erlaubt . Mindsphere weist jedoch timeseries mit mehr als 1 MB ab. sieh MDSPH doku HR19.11.18
	    if (max_http_payload_size > 1024*1024)
	      max_http_payload_size = 1024*1024; // F�r Files sind 10MB erlaubt . Mindsphere weist jedoch timeseries mit mehr als 1 MB ab. sieh MDSPH doku HR19.11.18

}

static E_MCL_CORE_RETURN_CODE _add_data_model(char **configuration_id)//,mcl_data_source_configuration_t** data_source_configuration)
{
	int i = 0;
	int j = 0;
	int k = 0;
	E_MCL_CORE_RETURN_CODE code;

    code = mcl_data_source_configuration_initialize(MCL_DATA_SOURCE_CONFIGURATION_1_0, &data_source_configuration);

    if (code != MCL_OK) return code;

    code = mcl_random_generate_guid(configuration_id);
    code = mcl_data_source_configuration_set_parameter(data_source_configuration, MCL_DATA_SOURCE_CONFIGURATION_PARAMETER_ID, *configuration_id);
    if (code != MCL_OK) return code;

    sprintf(szPrintOut,"DataSourceConfiguration:Version %s - ID: %s\n", szDataSourceVersion , *configuration_id); agentdprint(szPrintOut);

    // configuration ID wird spaeter in Config File gespeichert!

    for (i = 0; i < nDataSrc_Count ; i++)
    {


    	mcl_json_t *data_source_custom_data = NULL;
        code = mcl_json_util_initialize(MCL_JSON_OBJECT, &data_source_custom_data);
        code = mcl_json_util_add_string(data_source_custom_data, szdscf[i], szdscv[i]); //"dscf", "dscv");

        //code = mcl_data_source_configuration_add_data_source(data_source_configuration, szDataSrc_Name[i], szDataSrc_Description[i], NULL/*data_source_custom_data*/, &data_source[i]);
    	data_source = NULL;
    	code  = mcl_data_source_initialize(&data_source);

    	code = mcl_data_source_set_parameter(data_source, MCL_DATA_SOURCE_PARAMETER_NAME, szDataSrc_Name[i]);
    	code += mcl_data_source_set_parameter(data_source, MCL_DATA_SOURCE_PARAMETER_DESCRIPTION, szDataSrc_Description[i]);
    	code += mcl_data_source_set_parameter(data_source, MCL_DATA_SOURCE_PARAMETER_CUSTOM_DATA,data_source_custom_data);

    	if (code != MCL_OK) return code;

    	sprintf(szPrintOut,"DataSource %d: Name: %s - Description: %s\n",  (i+1) , szDataSrc_Name[i], szDataSrc_Description[i]); agentdprint(szPrintOut);


        mcl_data_point_t *data_point        = NULL;
    	mcl_json_t *data_point_custom_data  = NULL;

    	for (j = 0 ; j < nDataSrc_DataPointsCount[i]; j++, k++)
		{
            data_point        = NULL;
    	    data_point_custom_data  = NULL;
            code = mcl_json_util_initialize(MCL_JSON_OBJECT, &data_point_custom_data);
            code = mcl_json_util_add_string(data_point_custom_data, szdpcf[k], szdpcv[k]);

            code  = mcl_data_point_initialize(&data_point);

    		if (!code) 	code += mcl_data_point_set_parameter(data_point, MCL_DATA_POINT_PARAMETER_ID, szDataPt_ID[k]);
            if (!code) 	code += mcl_data_point_set_parameter(data_point, MCL_DATA_POINT_PARAMETER_NAME, szDataPt_Name[k]);
            if (!code) 	code += mcl_data_point_set_parameter(data_point, MCL_DATA_POINT_PARAMETER_TYPE, szDataPt_DataType[k]);
            if (!code) 	code += mcl_data_point_set_parameter(data_point, MCL_DATA_POINT_PARAMETER_UNIT, szDataPt_Unit[k]);
            if (!code) 	code += mcl_data_point_set_parameter(data_point, MCL_DATA_POINT_PARAMETER_DESCRIPTION, szDataPt_Description[k]);
            if (!code)  code += mcl_data_point_set_parameter(data_point, MCL_DATA_POINT_PARAMETER_CUSTOM_DATA, data_point_custom_data);

            if (!code)
                code += mcl_data_source_add_data_point(data_source, data_point);
            else
                return code;

            sprintf(szPrintOut,"Datapoint %d %03d: %s - %s - %s - %s - %s\n",  (i+1)*1000 + (j+1), k+1 , szDataPt_ID[k], szDataPt_Name[k], szDataPt_Description[k], szDataPt_DataType[k], szDataPt_Unit[k]);agentdprint(szPrintOut);

			if (code != MCL_OK)
                return code;
		}
		code = mcl_data_source_configuration_add_data_source(data_source_configuration, data_source);
    }
    return code;

}

mcl_timeseries_t *time_series = NULL;
mcl_timeseries_value_list_t *timeseries_value_list = NULL;
mcl_timeseries_value_t *timeseries_value = NULL;
static mcl_error_t _add_resource_usage( char *szVal, char *szDT, char *szQuality, int n, int newTS)
{
    static long nTs = 0;
    mcl_error_t code;
    mcl_timeseries_value_t *timeseries_value = NULL;

    if (newTS) // Neuer Zeitstempel bedingt neue Timeseries
    {

    	// add timeseriesvaluelist to timeseries
        if (timeseries_value_list != NULL)
        {

            code = mcl_timeseries_add_value_list(time_series, timeseries_value_list);
            //sprintf(szPrintOut," mcl_timeseries_add_value_list(a): Code: %d -> %s\n",code ,MCL_CORE_CODE_TO_STRING(code));   agentdprint(szPrintOut);

            timeseries_value_list = NULL;

            if (code != MCL_OK )
            {
                sprintf(szPrintOut," mcl_timeseries_add_value_list failed: Code: %d -> %s --> Agent exits \n",code ,MCL_CORE_CODE_TO_STRING(code));   agentdprint(szPrintOut);
                return code;
            }
        }

    	// Initialize timeseries value list.
        code = mcl_timeseries_value_list_initialize(&timeseries_value_list);
                	// Set timestamp for timeseries value list.
        code += mcl_timeseries_value_list_set_parameter(timeseries_value_list, MCL_TIMESERIES_VALUE_LIST_PARAMETER_TIMESTAMP_ISO8601, szDT);

    	if (code == MCL_OK)
    	{
    		sprintf(szPrintOut,"Time Series: (%ld) successfully created\n",nTs++);agentdprint(szPrintOut);
    	}

    }

    // Initialize timeseries value.
    code = mcl_timeseries_value_initialize(&timeseries_value);
    //mcl_time_series_add_value(timeseries_value_list, szDataPt_ID[n], szVal, szQuality);
    code += mcl_timeseries_value_set_parameter(timeseries_value, MCL_TIMESERIES_VALUE_PARAMETER_DATA_POINT_ID, szDataPt_ID[n]);
    code += mcl_timeseries_value_set_parameter(timeseries_value, MCL_TIMESERIES_VALUE_PARAMETER_QUALITY_CODE, szQuality);
    code += mcl_timeseries_value_set_parameter(timeseries_value, MCL_TIMESERIES_VALUE_PARAMETER_VALUE, szVal);

    // Add timeseries value to timeseries value list.

    code += mcl_timeseries_value_list_add_value(timeseries_value_list, timeseries_value);

    timeseries_value = NULL;

    if (code != MCL_OK)
        {sprintf(szPrintOut,"Time Series: (%ld) FAIL -> set parameter\n",nTs);agentdprint(szPrintOut);}

    return code;


}
void set_optional_vars_2_zero(void)
{
    int i;

    if (strncmp (szSubTenantDL,"Optional", 8) == 0)
        szSubTenantDL[0] = 0;

    if (strncmp (szUpLoadUrlDL,"Optional", 8) == 0)
        szUpLoadUrlDL[0] = 0;

    for (i = 0 ;  i < nDataSrc_Count;  i++)
	{
        if (strncmp (szdscf[i],"Optional", 8) == 0)
        {
            szdscf[i][0] = 0;
        }
        if (strncmp (szdscv[i],"Optional", 8) == 0)
        {
            szdscv[i][0] = 0;
        }
	}

	for (i = 0 ;  i < nDataPoints;  i++)
    {
        if (strncmp (szdpcf[i],"Optional", 8) == 0)
        {
            szdpcf[i][0] = 0;
        }
        if (strncmp (szdpcv[i],"Optional", 8) == 0)
        {
            szdpcv[i][0] = 0;
        }
        if (strncmp (szDataPt_Map2AssetID[i],"Optional", 8) == 0)
        {
            szDataPt_Map2AssetID[i][0] = 0;
        }
        if (strncmp (szDataPt_Map2AspectSet[i],"Optional", 8) == 0)
        {
            szDataPt_Map2AspectSet[i][0] = 0;
        }
        if (strncmp (szDataPt_Map2AspectVar[i],"Optional", 8) == 0)
        {
            szDataPt_Map2AspectVar[i][0] = 0;
        }
    }



}

int main(int argc, char *argv[] )
{


	// mcl_event_list_t *event_list;
	//mcl_store_t *store = NULL;
	//mcl_data_source_configuration_t *data_source_configuration = NULL;
	mcl_error_t code;



	config_init(&cfg);

	printf ("Raspberry Pi - MindSphere Agent %s \n\n", szRpiAgentVersion);
	printf ("ONLY for demonstration - not for commercial operation\n");
	printf ("ABSOLUTELY no warranty or liability for errors or data loss or resulting costs \n\n");
	printf (" -> Usage strictly on your own responsibility <-\n");
	printf ("By using the Agent you agree to this Terms\n\n\n");




	szstore_path 	   	= (char*) malloc(255); strcpy (szstore_path,			"/home/pi/");
	config_file 	   	= (char*) malloc(255); strcpy (config_file,			    "AgentConfigV4.cfg"); // Fix
	szRegistrationFile 	= (char*) malloc(255); strcpy (szRegistrationFile, 	    "registrationInformationV4.txt");

	szDataInputFile    	= (char*) malloc(255); strcpy (szDataInputFile, 		"/tmp/RPiAgentDataInput.txt");
	szEventInputFile   	= (char*) malloc(255); strcpy (szEventInputFile, 	    "/tmp/RPiAgentEventInput.txt");
	szUpLoadDir			= (char*) malloc(255); strcpy (szUpLoadDir, 	        "/tmp/RPiAgentFiles2Upload/");
	szUpLoadDirDL	    = (char*) malloc(255); strcpy (szUpLoadDirDL, 	        "/tmp/RPiAgentDataLakeFiles2Upload/");
	szSubTenantDL	    = (char*) malloc(255); strcpy (szSubTenantDL, 	        "Optional DataLakeSubTenantID");
	szUpLoadUrlDL       = (char*) malloc(255); strcpy (szUpLoadUrlDL, 	        "Optional DataLakeUploadURL");

	szAgentDebugFile   	= NULL;//(char*) malloc(255); strcpy (szAgentDebugFile, 	"/tmp/AgentDebugTrace.txt");
	szMclDebugFile     	= NULL;//(char*) malloc(255); strcpy (szMclDebugFile, 	"/tmp/MclDebugTrace.txt");




	if (argc >=2)
	{
		strcpy (szstore_path ,argv[1]);

	}
	char *szTmp1 = malloc(500);
	sprintf(szTmp1,"%s%s",szstore_path,config_file); 			strcpy (config_file, szTmp1);
	sprintf(szTmp1,"%s%s",szstore_path,szRegistrationFile); 		strcpy (szRegistrationFile, szTmp1);
	chknfree (szTmp1);

	ReadAgentConfiguration();



	if (strcmp (szinitial_access_token,"Agent is onboarded") != 0) // wenn ein neuer IAT eingef�gt wird, wird das alte registrierungsfile hier gel�scht...
		unlink (szRegistrationFile);							   // ...bevor das onboarding passiert. nach erfolgreichen Onboarding wird wieder dieser string eingetragen.


	if ((strstr (strlwr(szCfg_MdSphUrl),".eu") == NULL) && (strstr (strlwr(szCfg_MdSphUrl),".cn") == NULL))
	{
		sprintf(szPrintOut, "RPi 4.x Agent only works with MindSphere V3\nWrong Tenant address: %s \nThe web address must contain: ->.eu or.cn <-\n\nAgent exits\n", szCfg_MdSphUrl); agentdprint(szPrintOut);
		cleanup();
		exit (-1);
	}


	int nSleep = 10;
	for (nSleep = nAgentStartDelay ; nSleep > 0 ; nSleep--)
	{
		printf ("Starting the agent in  > %02d < seconds    (Press Ctrl-C to exit)", nSleep); fflush(stdout);
		sleep (1);
		printf ("\r"); fflush(stdout);
	}


	if (szMclDebugFile != NULL)
		unlink(szMclDebugFile);		// Alte Debug Datei ggf. loeschen

    mcl_log_util_set_callback(log_function,NULL);

	code = mcl_core_configuration_initialize(&core_configuration);
	_setup_configuration(core_configuration);//file:///home/pi/MCL401/mcl_core/src/core_processor.c

	code = mcl_core_initialize(core_configuration, &core);


    if (MCL_OK == code)
	{
        // perform onboard
    	code = mcl_core_onboard(core);

        if (code == MCL_OK)
		{
            sprintf(szPrintOut, "Agent successfully onboarded.\n"); agentdprint(szPrintOut);
            CfgGroup = config_setting_get_member(CfgRoot,"MCL-Config");
            CfgMember = config_setting_get_member(CfgGroup,"Initial_Access_Token");
            config_setting_set_string(CfgMember, "Agent is onboarded");
            config_write_file(&cfg, config_file);
        }
		else if (MCL_ALREADY_ONBOARDED == code)
		{
            sprintf(szPrintOut,"Agent is already onboarded!\n"); agentdprint(szPrintOut);
        }

        if ((MCL_OK == code) || (MCL_ALREADY_ONBOARDED == code))
		{
            //code = mcl_core_get_access_token(core);
            code = get_access_token(core);            //code = mcl_core_rotate_key(core);
            //code = mcl_core_get_access_token(core);
            if ((code == 0)|| (MCL_ALREADY_ONBOARDED == code))
			{
                sprintf(szPrintOut,"Authorization Key successfully rotated.\n"); agentdprint(szPrintOut);
            }
			else
			{
                sprintf(szPrintOut,"Key rotation failed code = %d\n", code); agentdprint(szPrintOut);
                mcl_connectivity_destroy(&connectivity);
                return code;
            }

            // init store
            //code = mcl_store_initialize( &store);
            code = mcl_connectivity_configuration_initialize(&connectivity_configuration);
            code = mcl_connectivity_configuration_set_parameter(connectivity_configuration, MCL_CONNECTIVITY_CONFIGURATION_PARAMETER_CORE, core);
            code = mcl_connectivity_initialize(connectivity_configuration, &connectivity);

            //code = mcl_connectivity_configuration_set_parameter(connectivity_configuration, MCL_CONNECTIVITY_CONFIGURATION_PARAMETER_MAX_HTTP_PAYLOAD_SIZE, &maximum_payload_size);

            // Send data model

            if (strncmp(szDataConfig_ID, "WillBeDoneByAgent",17) == 0)
            {

				code = _add_data_model(&szDataConfig_ID);// ,&data_source_configuration);
				if (code == MCL_OK)
				{
					agentdprint("Data model successfully created...\n");
				}
				else
				{
					sprintf(szPrintOut,"Creation of data model failed code = %d\n", code); agentdprint(szPrintOut);
					mcl_connectivity_destroy(&connectivity);
					return code;
				}

				code = mcl_connectivity_exchange(connectivity, data_source_configuration);

				if (code == MCL_OK)
				{
					CfgGroup = config_setting_get_member(CfgRoot,"Data-Model");
					CfgMember = config_setting_get_member(CfgGroup,"DataConfig_ID");
					config_setting_set_string(CfgMember, szDataConfig_ID);
					config_write_file(&cfg, config_file);
					sprintf(szPrintOut,"Data model successfully uploaded with new DSC ID: %s\n", szDataConfig_ID); agentdprint(szPrintOut);
					nNewDataModelCreated++;
				}
				else
				{
					sprintf(szPrintOut,"Exchange of data model failed code = %d\n", code);agentdprint(szPrintOut);
					mcl_connectivity_destroy(&connectivity);
					return code;
				}
            }
            else
            	{sprintf(szPrintOut,"Using existing Data Source Configuration -> ID: %s\n", szDataConfig_ID);agentdprint(szPrintOut);}

            // Get events - entfallen bei V3
		}
        else
        {
        	mcl_connectivity_destroy(&connectivity);
            sprintf(szPrintOut," Agent Authentication failed: Code: %d -> %s --> Agent exits \n",code ,MCL_CORE_CODE_TO_STRING(code));   agentdprint(szPrintOut);
            for (code = 0;code <34; code++)
            {
                sprintf(szPrintOut," Agent Authentication failed: Code: %d -> %s --> Agent exits \n",code,MCL_CORE_CODE_TO_STRING(code));  agentdprint(szPrintOut);
            }
        	return code;

        }

        if (data_source_configuration != NULL)
            mcl_data_source_configuration_destroy(&data_source_configuration);

        // mcl_store_destroy(&store); // Wird gleich wieder neu initialisiert

        manage_data_mapping(connectivity);

        set_optional_vars_2_zero(); //



        int nDps;
        for (nDps = 0; nDps < MAXDP; nDps++)
        {
        	dOldVal[nDps] = 9999999.99999 ; // Nonsense wert , damit der erste Wert auf alle F�lle eingelesen wird.
        	lOldTime[nDps] = 0;
        }

        FILE *fpT;
        char szDataInputFileNew[255];

        if (strlen(szDataInputFile) < 4)
            strcat(szDataInputFile,"1234");

        strcpy (szDataInputFileNew,szDataInputFile);
        strcpy(&(szDataInputFileNew[strlen(szDataInputFileNew)-4]), "Tmp.txt");



        while (1)
        {

            manage_file_upload(connectivity); // File Upload aus vorgegebenen Directory
            manage_events(connectivity); // Event Management
            manage_data_lake_file_upload(core);


            rename(szDataInputFile,szDataInputFileNew);
            fpT = fopen(szDataInputFileNew, "r");

            if (fpT == NULL)
            {
            	if (nSleepPts == 0)
            		agentdprint("Data Input File(s) not available -> Data acquisation Script(s) running ? -> Agent sleeping (Press Ctrl-C to exit)");
            	else
            		printf(".");

            	fflush(stdout);
            	nSleepPts++;

            	sleep (nAgentSleepTime);
            	continue;

            }
            nSleepPts = 0;

           // code = mcl_store_initialize(&store);

            time_series = NULL;
            code  = mcl_timeseries_initialize(MCL_TIMESERIES_VERSION_1_0, &time_series);
            code += mcl_timeseries_set_parameter(time_series, MCL_TIMESERIES_PARAMETER_CONFIGURATION_ID, szDataConfig_ID);


            char  szVal[100];
            char  szDT[100];
            char  szT1[100];
            char  szT2[100];

            int  bSend = 0;
            int  nLinesToSendCtr= 0;
            int  nDataNr 	= 1;
            int  nSendCollectedTS = 0; // Timeseries sammeln
            int  nAddedDP = 0;			// Maximal MAXDPTS Values insgesamt sonst l�uft die Payload �ber.
            char szQuality[20] ;
            fpos_t  fpos;
            char *pDP, *pQ, *pV, *pT;

            char* pWorktodo = NULL;			// Data Inputstring
            								// DPno Quality    Value      Date Time
			while(1)						// 1001|0000000000|   23.9370|2018-04-06 13:04:02.987550
											// 01234012345678900123456789001234567890123456788901234
			{								// 0123456789012345678901234567890123456789012345678901234567890
											//           1         2         3         4         5         6
				int nLinesCtr = 0;
				int nInputLinesCtr= 0;

				if (time_series == NULL)
				{
                    code  = mcl_timeseries_initialize(MCL_TIMESERIES_VERSION_1_0, &time_series);
                    code += mcl_timeseries_set_parameter(time_series, MCL_TIMESERIES_PARAMETER_CONFIGURATION_ID, szDataConfig_ID);
                }

				while ((pWorktodo = fgets (szTmp[nInputLinesCtr],99,fpT)) != NULL )//| nWork) // ganze Zeile holen
				{
					char szTmpAct[100];
					strcpy(szTmpAct,szTmp[nInputLinesCtr]);

					// Split the Input String
					pDP = szTmpAct;
					if ( pT == NULL) continue;

					pQ = strchr(pDP,'|'); 	if ( pQ == NULL) goto FALSE_INPUT_LINE1;

					*pQ = 0; pQ++;
					pV = strchr(pQ,'|'); 	if ( pV == NULL) goto FALSE_INPUT_LINE1;

					*pV = 0; pV++;
					pT = strchr(pV,'|'); 	if ( pT == NULL) goto FALSE_INPUT_LINE1;
					*pT = 0; pT++;

FALSE_INPUT_LINE1:
					if ( pDP == NULL || pQ == NULL || pV == NULL || pT == NULL)
					{
						sprintf (szPrintOut,"Invalid Input String: Format faulty -> Ignoring: %s\n", szTmp[nInputLinesCtr]);agentdprint(szPrintOut);
												continue;
					}

					int nTsp;
					for (nTsp = TimeStamp_Precision ; nTsp > 0 ; nTsp--)
						*(pT+22-nTsp) = 0;

					//szTmp[nInputLinesCtr][50-nTsp] = '0'; //[31] ='Z'


					// ist es der gleiche Zeitstempel ? Dann kann es in eine Timeseries rein
					// Maximal sind MAXDP unterschiedlich Datapoints mit identischen Zeitstempel möglich
					strcpy(szT1,pT); 	szT1[10]= 'T'; 	szT1[23]= 'Z'; 	szT1[24]= 0; // für Mindsphere Zeit Format anpassen


					if (nInputLinesCtr > 0)
					{
						if (strcmp (szT1,szT2))// zeit vergleichen , 	wenn unterschiedlich beenden
						{
							fsetpos (fpT,&fpos); // File Lesezeiger restaurieren , da unterschied , also neuer Zeitstempel
							break;
						}
					}
					strcpy (szT2, szT1);
					fgetpos (fpT,&fpos);

					//DsNrOld = DsNr;

					nInputLinesCtr++;
					nLinesToSendCtr++;

					if ((nLinesToSendCtr >= (max_http_payload_size/4000))  || (nLinesToSendCtr >= 99)) // 1 datapoint = 1000 Byte  oversized ... MAXDPTS = (max_http_payload_size/1000))
					{
						nSendCollectedTS++; // Mehr als MaxDP zeilen werden icht eingelesen ... daher
						break;
					}

				}

				if ((pWorktodo == NULL) && (nInputLinesCtr == 0)) // ende des Input files erreicht
					break;

				if ((pWorktodo == NULL) && (nInputLinesCtr > 0)) // ende des Input files erreicht, die letzten Zeilen / Dates�tze wurden gelesen
					nSendCollectedTS++;


				int newTS = 1; // Neue Timeseries anlegen
				int offset = 0;
				for  (nLinesCtr= 0; nLinesCtr < nInputLinesCtr; nLinesCtr++ ) // Alle werte in einem neuen Zeitstempel bearbeiten
				{
					int ds,dp;
					char szTyp[30];

					char szTmpAct[100];
					strcpy(szTmpAct,szTmp[nLinesCtr]);

					// Split the Input String
					pDP = szTmpAct;
					pQ = strchr(pDP,'|');	if ( pQ == NULL) goto FALSE_INPUT_LINE2;

					*pQ = 0; pQ++;
					pV = strchr(pQ,'|');	if ( pV == NULL) goto FALSE_INPUT_LINE2;
					*pV = 0; pV++;
					pT = strchr(pV,'|'); 	if ( pT == NULL) goto FALSE_INPUT_LINE2;
					*pT = 0; pT++;



	FALSE_INPUT_LINE2:
					if ( pDP == NULL || pQ == NULL || pV == NULL || pT == NULL)
					{
						sprintf (szPrintOut,"Invalid Input String: Format faulty -> Ignoring: %s\n", szTmp[nLinesCtr]);agentdprint(szPrintOut);
												continue;
					}

					lDataPoints++;

					szTmp[nLinesCtr] [strlen(szTmp[nLinesCtr])-1] = 0;

					nDataNr = atoi(pDP); //(szTmp[nLinesCtr]);

					//Offset ausrechnen und dann in nDataNr speichern
					ds = nDataNr / 1000;
					dp = nDataNr- (ds * 1000);

					if ((nDataNr < 1001) || (nDataNr > MAXDS*1000+MAXDP))
					{
						sprintf (szPrintOut,"Invalid Data Number: Outside of Range (1001-9999) -> Ignoring: %s\n", szTmp[nLinesCtr]);agentdprint(szPrintOut);
						continue;
					}


					if (ds > nDataSrc_Count)
					{
						sprintf (szPrintOut,"Invalid Data Source Number: -> Ignoring: %s\n", szTmp[nLinesCtr]);agentdprint(szPrintOut);
						continue;
					}

					if (dp > nDataSrc_DataPointsCount[ds-1])
					{
						sprintf (szPrintOut,"Invalid Data Point Number: -> Ignoring: %s\n", szTmp[nLinesCtr]);agentdprint(szPrintOut);
						continue;
					}


					int ctr, found = 0;
					for (ctr = 0 ; ctr < nDataPoints ; ctr++)
					{
						if (nDataPtInCfg[ctr] == nDataNr)
						{
							found = 1;
							break;
						}
					}

					if (! found)
					{
						sprintf (szPrintOut,"Invalid Data Point Number: -> Ignoring: %s\n", szTmp[nLinesCtr]);agentdprint(szPrintOut);
						continue;
					}


					ds--;
					offset = 0;
					for (ctr = 0 ; ctr < ds ; ctr++)
					{
						offset += nDataSrc_DataPointsCount[ctr];
					}
					offset += dp;
					offset -= 1;

					if (offset > nDataPoints-1 )
					{
						sprintf (szPrintOut,"Invalid Data Number: Not in ConfigFile -> Ignoring: %s <- Offset:%d\n", szTmp[nLinesCtr], offset);agentdprint(szPrintOut);
						continue;
					}

					strcpy(szTyp,szDataPt_DataType[offset]);
					strlwr(szTyp);

					// Die Datentypen der Data Points werden schon beim Einlesen der Config gepr�ft !

					//Quality aus dem Inputstring holen
					strcpy(szQuality,pQ);

					// wenn Datatyp = string , big_string , timestamp ist .... dann besonders behandeln
					found = 0;
					if 	(!strcmp (szTyp,"boolean"))
					{
						found++; strlwr(pV);
						if (strstr(pV,"true") != NULL) 		strcpy (pV,"1"); // wird sp�ter wieder in true / false r�ckgewandelt. so ist auch 0/1 als input m�glich
						if (strstr(pV,"false") != NULL) 	strcpy (pV,"0");
					}

					if	(!strcmp (szTyp,"int")) 		found++;
					if	(!strcmp (szTyp,"long"))    	found++;
					if	(!strcmp (szTyp,"double"))		found++;

					if (found)
					{
						dNewVal[offset] = atof(pV);		//&szTmp[nLinesCtr][16]);

						// Grenzwerte bestimmen

						strcpy(szDT,pT); szDT[10]= 'T';	szDT[23]= 'Z'; szDT[24]= 0; // Zeitstring bilden

						lNewTime[offset] = GetTimeInMs (szDT);		// Zeit in Tages MilliSekunden umrechnen

						if ((lNewTime[offset] - lOldTime[offset]) < 0) // Tageswechsel ?
								lOldTime[offset] -= 86400000; // Tages Millisekunden


						if (lNewTime[offset] < (lOldTime[offset] + (1000 * nMaxDeltaTime[offset]))) // Wenn der Zeitraum kleiner als der maximale abstand von 2 Datenpunkten ist, kann man bei gleichen Werten das Senden sparen
						{															// Ist der Zeitraum grösser , wird auf alle Fälle gesendet
							double dDelta = atof (szMinDeltaValue[offset]); // in Prozent
							double dLow, dHigh;

							if (dDelta == 0) // Rundungsfehler bei delta 0% vermeiden...
							{
								if (dNewVal[offset] == dOldVal[offset])
									continue;
							}
							else // Prozentgrenzen ausrechnen und Bereich checken
							{
								dLow  = dOldVal[offset] - (0.01* dOldVal[offset] * dDelta);
								dHigh = dOldVal[offset] + (0.01* dOldVal[offset] * dDelta);

								if ((dLow <= dNewVal[offset]) && (dNewVal[offset] <= dHigh))
									continue;
							}
						}
						else
						{
							lOldTime[offset] = lNewTime[offset];
						}
						dOldVal[offset] = dNewVal[offset];

						// Ausgabe auf Datentyp normieren
						if (!strcmp (szTyp,"boolean"))
						{
							if ((_Bool) dNewVal[offset])
								sprintf(szVal, "true");
							else
								sprintf(szVal, "false");
						}
						if (!strcmp (szTyp,"int"))
							sprintf(szVal, "%d", (int)dNewVal[offset]);
						if (!strcmp (szTyp,"long"))
							sprintf(szVal, "%ld", (long)dNewVal[offset]);
						if (!strcmp (szTyp,"double"))
							sprintf(szVal, "%f", dNewVal[offset]);

					} // oberhalb nur nur boolean , int , long , double bearbeiten
					else // string typen
					{
						found = 0;
						if	(!strcmp (szTyp,"string")) 		found++;
						if	(!strcmp (szTyp,"big_string")) 	found++;
						if	(!strcmp (szTyp,"timestamp"))	found++;

						if (found)
							strcpy(szVal, pV);
						// else unbekannter Datentyp , wird schon beim Check der Config abgefangen !
					}

					lDataPointsSent++;
					code = _add_resource_usage(szVal, szDT, szQuality,offset,newTS);
					lDataPointsSentOk++;

					if (nAddedDP++ > (max_http_payload_size/4000)) // 1 datapoint = 1000 Byte  oversized ... MAXDPTS = (max_http_payload_size/1000)
						nSendCollectedTS++;

					//if (sizeof(*store) > max_http_payload_size - 2000)
					//	nSendCollectedTS++;
					if (code == MCL_OK)
                        {sprintf(szPrintOut,"Data Point: (%04d)(%03d) successfully added, Total DP:(%ld) ... Val: %10s Time: %s\n",nDataNr, offset+1, lDataPointsSentOk, szVal,szDT);agentdprint(szPrintOut);}

					newTS = 0;
					bSend++;	// Optimierung: bSend nur auf 1 setzen , wenn das Ende der Input Datei erreicht ist ,oder sizeof store knapp unter dem Maximum ist. Gut für grosse Mengengerüste.
				} // end for




				int nSndCtr = 3;
				if (bSend && nSendCollectedTS)
				{

                    if (timeseries_value_list != NULL)
                    {

                        code = mcl_timeseries_add_value_list(time_series, timeseries_value_list);
                        //sprintf(szPrintOut," mcl_timeseries_add_value_list(b): Code: %d -> %s\n",code ,MCL_CORE_CODE_TO_STRING(code));   agentdprint(szPrintOut);


                        if (code != MCL_OK )
                        {
                            sprintf(szPrintOut," mcl_timeseries_add_value_list failed: Code: %d -> %s --> Agent exits \n",code ,MCL_CORE_CODE_TO_STRING(code));   agentdprint(szPrintOut);
                            return code;
                        }
                    }



					while( nSndCtr > 0)
					{
						struct timespec tstart={0,0}, tend={0,0};
						double tt;

						nSndCtr--;
						nInputLinesCtr = 0;

						//mcl_log_util_set_output_level(1);

						clock_gettime(CLOCK_MONOTONIC, &tstart);
						code = mcl_connectivity_exchange(connectivity, time_series);
                        clock_gettime(CLOCK_MONOTONIC, &tend);

						//mcl_log_util_set_output_level(4);

						lUploadsSent++;

						if (code == MCL_OK)
						{
							lUploadsSentOk++;
							nLinesToSendCtr= 0;
							bSend = 0;
							nSendCollectedTS = 0;
							nAddedDP = 0;
							tt = ((double)tend.tv_sec + 1.0e-9*tend.tv_nsec) - ((double)tstart.tv_sec + 1.0e-9*tstart.tv_nsec);

							sprintf(szPrintOut,"Data Store (%ld) successfully uploaded...in %f s / ServerErrors since Start: %ld\n",lUploadsSentOk, tt, lServerError); agentdprint(szPrintOut);
							mcl_timeseries_destroy(&time_series);
							time_series = NULL;
							timeseries_value_list = NULL;
							break; // erfolgreich gesendet , raus ...
						}
						// If unauthorized error received, call rotate key to generate a new authentication key.
						else if (MCL_UNAUTHORIZED == code)
						{
							lRotateKeys++;
							nSndCtr++;
							agentdprint("Key rotation is being done : ");
							code = mcl_core_rotate_key(core);
							// danach timeseries noch mal senden ...
							sprintf(szPrintOut, "%s.\n", MCL_CORE_CODE_TO_STRING(code)); agentdprint(szPrintOut);

							if (code == MCL_OK)
                                code = mcl_core_get_access_token(core);

							if (code == MCL_OK)
                                nSndCtr++;

                            if ( nSndCtr == 0)  // nSndCtr (3) Versuche
                            {
								sprintf(szPrintOut,"Key rotation failed: Code: %d -> %s --> Agent exits \n",code ,MCL_CORE_CODE_TO_STRING(code));   agentdprint(szPrintOut);
								mcl_connectivity_destroy(&connectivity);
								fclose (fpT);
								return code; // fataler Fehler -> Agent beenden
							}
						}
						else
						{
							lServerError++;
							sprintf(szPrintOut,"Upload of Data Store (%ld) failed: Code: %d -> %s --> sleeping %ds \n",lUploadsSent, code ,MCL_CORE_CODE_TO_STRING(code),nAgentSleepTime);   agentdprint(szPrintOut);
							sleep(nAgentSleepTime);

							// Internet connection broken ?  Repeat until internet is back again
							if  (code == MCL_COULD_NOT_RESOLVE_HOST ||  code == MCL_COULD_NOT_CONNECT)
								nSndCtr++;


							if ( nSndCtr == 0)  // nSndCtr (3) Versuche
							{
								mcl_connectivity_destroy(&connectivity);
								fclose (fpT);
								return code; // fataler Fehler -> Agent beenden
							}
						}
					}// while (sndctr)
				}// if bSend
			} // while 1

			fclose (fpT);
            unlink (szDataInputFileNew); // L�schen

            sprintf(szPrintOut,"Datapoints total: %ld , sent: %ld , OK: %ld / uploaded Data Stores total: %ld , sent OK: %ld /Key rotations: %ld --> sleeping %ds\n", lDataPoints, lDataPointsSent, lDataPointsSentOk, lUploadsSent, lUploadsSentOk, lRotateKeys, nAgentSleepTime); agentdprint(szPrintOut);
            sleep(nAgentSleepTime);
        } // while 1

        mcl_connectivity_destroy(&connectivity);
        // mcl_log_util_finalize(); tobedone !
    	agentdprint("No Data Input File available -> Data acquisation Script running ?\n\nAgent exits\n");

    	cleanup();

    	config_destroy(&cfg); // muss ganz zuletzt kommen sonst sind diverse variablen nicht mehr gültig
    	mcl_core_configuration_destroy(&core_configuration);

    }
    return code;
}






