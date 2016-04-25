# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# update.py
# Created on: 2016-03-23
# The below code was modified from the original developed by ESRI found here 
# https://github.com/arcpy/update-hosted-feature-service. Changes were made in 
# support of Chesterfield County VA's ArcGIS Online implementation. If you have
# any questions feel free to email CCArcGIS@chesterfield.gov and the current
# GIS Analyst will respond.
#
# Description: 
# This script reads through all the .ini files and updates the services that 
# go with them.
# -----------------------------------------------------------------------------
# Import system modules
import ConfigParser
import ast
import os
import sys
import time
import urllib2
import urllib
import json
import mimetypes
import gzip
from io import BytesIO
import string
import random
import re
from xml.etree import ElementTree as ET
import logging
import logging.handlers
import arcpy
import datetime

# Set the logger name
loggerName = 'etl.update'
logger = logging.getLogger(loggerName)
genFormat = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s','%Y-%m-%d %H:%M')

# Set the name of the log file, you can call this whatever you like.
logFileName = 'update.log'
# Set the log file path, the log will created and stored in the same directory that the script resides.
logFilePath = r'C:\Path\To\Log'
logFile = os.path.join(logFilePath, logFileName)

# This log handler will output on the console
streamLogHandler = logging.StreamHandler()
streamLogHandler.setLevel(logging.DEBUG)
streamLogHandler.setFormatter(genFormat)
logger.addHandler(streamLogHandler)

# This log handler will output to the specified log file and
# will rotate it with two others when it gets to about 5Mb
rotateLogHandler = logging.handlers.RotatingFileHandler(logFile, mode='a', maxBytes=5000000, backupCount=2)
rotateLogHandler.setLevel(logging.DEBUG)
rotateLogHandler.setFormatter(genFormat)
logger.addHandler(rotateLogHandler)

logger.setLevel(logging.DEBUG)

start = datetime.datetime.now()

class AGOLHandler(object):

    def __init__(self, username, password, serviceName, folderName, proxyDict):
        self.logger = logging.getLogger(__name__)
        self.headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'User-Agent': ('updatehostedfeatureservice')
        }
        self.username = username
        self.password = password
        self.base_url = "https://www.arcgis.com/sharing/rest"
        self.proxyDict = proxyDict
        self.serviceName = serviceName
        self.token = self.getToken(username, password)
        self.itemID = self.findItem("Feature Service")
        self.SDitemID = self.findItem("Service Definition")
        self.folderName = folderName
        self.folderID = self.findFolder()

    def getToken(self, username, password, exp=60):

        referer = "http://www.arcgis.com/"
        query_dict = {'username': username,
                      'password': password,
                      'expiration': str(exp),
                      'client': 'referer',
                      'referer': referer,
                      'f': 'json'}

        token_url = '{}/generateToken'.format(self.base_url)

        token_response = self.url_request(token_url, query_dict, 'POST')

        if "token" not in token_response:
            self.logger.info(token_response['error'])
            sys.exit()
        else:
            return token_response['token']

    def findItem(self, findType):
        """ Find the itemID of whats being updated
        """

        searchURL = self.base_url + "/search"

        query_dict = {'f': 'json',
                      'token': self.token,
                      'q': "title:\"" + self.serviceName + "\"AND owner:\"" +
                      self.username + "\" AND type:\"" + findType + "\""}

        jsonResponse = self.url_request(searchURL, query_dict, 'POST')

        if jsonResponse['total'] == 0:
            self.logger.info("Could not find a service to update. Check the service name in the settings.ini")
            sys.exit()
        else:
            findid = jsonResponse['results'][0]["id"]
            self.logger.info("found {} : {}".format(findType, findid))
            return findid

    def findFolder(self, folderName=None):
        """ Find the ID of the folder containing the service
        """

        if self.folderName == "None":
            return ""

        findURL = "{}/content/users/{}".format(self.base_url, self.username)

        query_dict = {'f': 'json',
                      'num': 1,
                      'token': self.token}

        jsonResponse = self.url_request(findURL, query_dict, 'POST')

        for folder in jsonResponse['folders']:
            if folder['title'] == self.folderName:
                return folder['id']

        self.logger.info("Could not find the specified folder name provided in the settings.ini")
        self.logger.info("-- If your content is in the root folder, change the folder name to 'None'")
        sys.exit()

    def upload(self, fileName, tags, description):
        """
         Overwrite the SD on AGOL with the new SD.
         This method uses 3rd party module: requests
        """

        updateURL = '{}/content/users/{}/{}/items/{}/update'.format(self.base_url, self.username,
                                                                    self.folderID, self.SDitemID)

        query_dict = {"filename": fileName,
                      "type": "Service Definition",
                      "title": self.serviceName,
                      "tags": tags,
                      "description": description,
                      "f": "json",
                      'multipart': 'true',
                      "token": self.token}

        details = {'filename': fileName}
        add_item_res = self.url_request(updateURL, query_dict, "POST", "", details)

        itemPartJSON = self._add_part(fileName, add_item_res['id'], "Service Definition")

        if "success" in itemPartJSON:
            itemPartID = itemPartJSON['id']

            commit_response = self.commit(itemPartID)

            # valid states: partial | processing | failed | completed
            status = 'processing'
            while status == 'processing' or status == 'partial':
                status = self.item_status(itemPartID)['status']
                time.sleep(1.5)

            self.logger.info("updated SD:   {}".format(itemPartID))
            return True

        else:
            self.logger.info(".sd file not uploaded. Check the errors and try again.")
            self.logger.info(itemPartJSON)
            sys.exit()

    def _add_part(self, file_to_upload, item_id, upload_type=None):
        """ Add the item to the portal in chunks.
        """

        def read_in_chunks(file_object, chunk_size=10000000):
            """Generate file chunks of 10MB"""
            while True:
                data = file_object.read(chunk_size)
                if not data:
                    break
                yield data

        url = '{}/content/users/{}/items/{}/addPart'.format(self.base_url, self.username, item_id)

        with open(file_to_upload, 'rb') as f:
            for part_num, piece in enumerate(read_in_chunks(f), start=1):
                title = os.path.basename(file_to_upload)
                files = {"file": {"filename": file_to_upload, "content": piece}}
                params = {
                    'f': "json",
                    'token': self.token,
                    'partNum': part_num,
                    'title': title,
                    'itemType': 'file',
                    'type': upload_type
                }

                request_data, request_headers = self.multipart_request(params, files)
                resp = self.url_request(url, request_data, "MULTIPART", request_headers)

        return resp

    def item_status(self, item_id):
        """ Gets the status of an item.
        Returns:
            The item's status. (partial | processing | failed | completed)
        """

        url = '{}/content/users/{}/items/{}/status'.format(self.base_url, self.username, item_id)
        parameters = {'token': self.token,
                      'f': 'json'}

        return self.url_request(url, parameters)

    def commit(self, item_id):
        """ Commits an item that was uploaded as multipart
        """

        url = '{}/content/users/{}/items/{}/commit'.format(self.base_url, self.username, item_id)
        parameters = {'token': self.token,
                      'f': 'json'}

        return self.url_request(url, parameters)

    def publish(self):
        """ Publish the existing SD on AGOL (it will be turned into a Feature Service)
        """

        publishURL = '{}/content/users/{}/publish'.format(self.base_url, self.username)

        query_dict = {'itemID': self.SDitemID,
                      'filetype': 'serviceDefinition',
                      'overwrite': 'true',
                      'f': 'json',
                      'token': self.token}

        jsonResponse = self.url_request(publishURL, query_dict, 'POST')
        self.logger.info("successfully updated...{}...".format(jsonResponse['services']))

        return jsonResponse['services'][0]['serviceItemId']

    def enableSharing(self, newItemID, everyone, orgs, groups):
        """ Share an item with everyone, the organization and/or groups
        """

        shareURL = '{}/content/users/{}/{}/items/{}/share'.format(self.base_url, self.username,
                                                                  self.folderID, newItemID)

        if groups is None:
            groups = ''

        query_dict = {'f': 'json',
                      'everyone': everyone,
                      'org': orgs,
                      'groups': groups,
                      'token': self.token}

        jsonResponse = self.url_request(shareURL, query_dict, 'POST')

        self.logger.info("successfully shared...{}...".format(jsonResponse['itemId']))

    def url_request(self, in_url, request_parameters, request_type='GET',
                    additional_headers=None, files=None, repeat=0):
        """
        Make a request to the portal, provided a portal URL
        and request parameters, returns portal response.

        Arguments:
            in_url -- portal url
            request_parameters -- dictionary of request parameters.
            request_type -- HTTP verb (default: GET)
            additional_headers -- any headers to pass along with the request.
            files -- any files to send.
            repeat -- repeat the request up to this number of times.

        Returns:
            dictionary of response from portal instance.
        """

        if request_type == 'GET':
            req = urllib2.Request('?'.join((in_url, urllib.urlencode(request_parameters))))
        elif request_type == 'MULTIPART':
            req = urllib2.Request(in_url, request_parameters)
        else:
            req = urllib2.Request(
                in_url, urllib.urlencode(request_parameters), self.headers)

        if additional_headers:
            for key, value in list(additional_headers.items()):
                req.add_header(key, value)
        req.add_header('Accept-encoding', 'gzip')

        if self.proxyDict:
            p = urllib2.ProxyHandler(self.proxyDict)
            auth = urllib2.HTTPBasicAuthHandler()
            opener = urllib2.build_opener(p, auth, urllib2.HTTPHandler)
            urllib2.install_opener(opener)

        response = urllib2.urlopen(req)

        if response.info().get('Content-Encoding') == 'gzip':
            buf = BytesIO(response.read())
            with gzip.GzipFile(fileobj=buf) as gzip_file:
                response_bytes = gzip_file.read()
        else:
            response_bytes = response.read()

        response_text = response_bytes.decode('UTF-8')
        response_json = json.loads(response_text)

        if not response_json or "error" in response_json:
            rerun = False
            if repeat > 0:
                repeat -= 1
                rerun = True

            if rerun:
                time.sleep(2)
                response_json = self.url_request(
                    in_url, request_parameters, request_type,
                    additional_headers, files, repeat)

        return response_json

    def multipart_request(self, params, files):
        """ Uploads files as multipart/form-data. files is a dict and must
            contain the required keys "filename" and "content". The "mimetype"
            value is optional and if not specified will use mimetypes.guess_type
            to determine the type or use type application/octet-stream. params
            is a dict containing the parameters to be passed in the HTTP
            POST request.

            content = open(file_path, "rb").read()
            files = {"file": {"filename": "some_file.sd", "content": content}}
            params = {"f": "json", "token": token, "type": item_type,
                      "title": title, "tags": tags, "description": description}
            data, headers = multipart_request(params, files)
            """
        # Get mix of letters and digits to form boundary.
        letters_digits = "".join(string.digits + string.ascii_letters)
        boundary = "----WebKitFormBoundary{}".format("".join(random.choice(letters_digits) for i in range(16)))
        file_lines = []
        # Parse the params and files dicts to build the multipart request.
        for name, value in params.iteritems():
            file_lines.extend(("--{}".format(boundary),
                               'Content-Disposition: form-data; name="{}"'.format(name),
                               "", str(value)))
        for name, value in files.items():
            if "filename" in value:
                filename = value.get("filename")
            else:
                raise Exception("The filename key is required.")
            if "mimetype" in value:
                mimetype = value.get("mimetype")
            else:
                mimetype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            if "content" in value:
                file_lines.extend(("--{}".format(boundary),
                                   'Content-Disposition: form-data; name="{}"; filename="{}"'.format(name, filename),
                                   "Content-Type: {}".format(mimetype), "",
                                   (value.get("content"))))
            else:
                raise Exception("The content key is required.")
        # Create the end of the form boundary.
        file_lines.extend(("--{}--".format(boundary), ""))

        request_data = "\r\n".join(file_lines)
        request_headers = {"Content-Type": "multipart/form-data; boundary={}".format(boundary),
                           "Content-Length": str(len(request_data))}
        return request_data, request_headers


def makeSD(MXD, serviceName, tempDir, outputSD, maxRecords, tags, summary):
    """ create a draft SD and modify the properties to overwrite an existing FS
    """

    arcpy.env.overwriteOutput = True
    # All paths are built by joining names to the tempPath
    SDdraft = os.path.join(tempDir, "tempdraft.sddraft")
    newSDdraft = os.path.join(tempDir, "updatedDraft.sddraft")

    # Check the MXD for summary and tags, if empty, push them in.
    try:
        mappingMXD = arcpy.mapping.MapDocument(MXD)
        if mappingMXD.tags == "":
            mappingMXD.tags = tags
            mappingMXD.save()
        if mappingMXD.summary == "":
            mappingMXD.summary = summary
            mappingMXD.save()
    except IOError:
        logger.info("IOError on save, do you have the MXD open? Summary/tag info not pushed to MXD, publishing may fail...")

    arcpy.mapping.CreateMapSDDraft(MXD, SDdraft, serviceName, "MY_HOSTED_SERVICES")

    # Read the contents of the original SDDraft into an xml parser
    doc = ET.parse(SDdraft)

    root_elem = doc.getroot()
    if root_elem.tag != "SVCManifest":
        raise ValueError("Root tag is incorrect. Is {} a .sddraft file?".format(SDDraft))

    # The following 6 code pieces modify the SDDraft from a new MapService
    # with caching capabilities to a FeatureService with Query,Create,
    # Update,Delete,Uploads,Editing capabilities as well as the ability
    # to set the max records on the service.
    # The first two lines (commented out) are no longer necessary as the FS
    # is now being deleted and re-published, not truly overwritten as is the
    # case when publishing from Desktop.
    # The last three pieces change Map to Feature Service, disable caching
    # and set appropriate capabilities. You can customize the capabilities by
    # removing items.
    # Note you cannot disable Query from a Feature Service.

    # doc.find("./Type").text = "esriServiceDefinitionType_Replacement"
    # doc.find("./State").text = "esriSDState_Published"

    # Change service type from map service to feature service
    for config in doc.findall("./Configurations/SVCConfiguration/TypeName"):
        if config.text == "MapServer":
            config.text = "FeatureServer"

    # Turn off caching
    for prop in doc.findall("./Configurations/SVCConfiguration/Definition/" +
                            "ConfigurationProperties/PropertyArray/" +
                            "PropertySetProperty"):
        if prop.find("Key").text == 'isCached':
            prop.find("Value").text = "false"
        if prop.find("Key").text == 'maxRecordCount':
            prop.find("Value").text = maxRecords

    # Turn on feature access capabilities
    for prop in doc.findall("./Configurations/SVCConfiguration/Definition/Info/PropertyArray/PropertySetProperty"):
        if prop.find("Key").text == 'WebCapabilities':
            prop.find("Value").text = "Query,Create,Update,Delete,Uploads,Editing"

    # Add the namespaces which get stripped, back into the .SD
    root_elem.attrib["xmlns:typens"] = 'http://www.esri.com/schemas/ArcGIS/10.1'
    root_elem.attrib["xmlns:xs"] = 'http://www.w3.org/2001/XMLSchema'

    # Write the new draft to disk
    with open(newSDdraft, 'w') as f:
        doc.write(f, 'utf-8')

    # Analyze the service
    analysis = arcpy.mapping.AnalyzeForSD(newSDdraft)

    if analysis['errors'] == {}:
        # Stage the service
        arcpy.StageService_server(newSDdraft, outputSD)
        logger.info("Created {}...".format(outputSD))

    else:
        # If the sddraft analysis contained errors, display them and quit.
        logger.info("Errors in analyze: \n {}...".format(analysis['errors']))
        sys.exit()

# The below function is the portion that starts to differ from the version made avalible at
# https://github.com/arcpy/update-hosted-feature-service you can see we gather a list of all the 
# FS_ServiceName.ini files in the directory you store them in. That list is used to update the
# Feature Service of each .ini file that is found. This seems to be a good way to do it because you
# can add or remove feature services and .ini file's as you need to without having to change your
# script.

def UpdateServiceDefinitionFile(localPath, inputUsername, inputPswd, proxyDict, iniFileLocation):
        
    FS_Files = [f for f in os.listdir(iniFileLocation) if re.match(r'[FS_]+.*\.ini', f)]
    FS_config = ConfigParser.ConfigParser()
    for ini_file in FS_Files:
        FS_config.read(ini_file)
        serviceName = FS_config.get('FS_INFO', 'SERVICENAME')
        folderName = FS_config.get('FS_INFO', 'FOLDERNAME')
        MXD = FS_config.get('FS_INFO', 'MXD')
        tags = FS_config.get('FS_INFO', 'TAGS')
        summary = FS_config.get('FS_INFO', 'DESCRIPTION')
        maxRecords = FS_config.get('FS_INFO', 'MAXRECORDS')
        orgs = FS_config.get('FS_SHARE', 'ORG')
        shared = FS_config.get('FS_SHARE', 'SHARE')
        everyone = FS_config.get('FS_SHARE', 'EVERYONE')
        groups = FS_config.get('FS_SHARE', 'GROUPS')  # Groups are by ID. Multiple groups comma separated

        # create a temp directory under the script
        tempDir = os.path.join(localPath, "tempDir")
        if not os.path.isdir(tempDir):
            os.mkdir(tempDir)
        finalSD = os.path.join(tempDir, serviceName + ".sd")

        # initialize AGOLHandler class
        agol = AGOLHandler(inputUsername, inputPswd, serviceName, folderName, proxyDict)

        # Turn map document into .SD file for uploading
        makeSD(MXD, serviceName, tempDir, finalSD, maxRecords, tags, summary)

        # overwrite the existing .SD on arcgis.com
        if agol.upload(finalSD, tags, summary):

            # publish the sd which was just uploaded
            fsID = agol.publish()

            # share the item
            if ast.literal_eval(shared):
                agol.enableSharing(fsID, everyone, orgs, groups)

# Here is where the actual processing begins to take place.
try:
    if __name__ == "__main__":
        logger.info("Starting the Service update process...")
        # Find and gather settings from the ini files
        localPath = sys.path[0]
        generalFile = os.path.join(localPath, "general.ini")

        # Creating the General Config parser to read the general.ini
        logger.info("Creating the general config parser...")
        if os.path.isfile(generalFile):
            General_config = ConfigParser.ConfigParser()
            General_config.read(generalFile)
        else:
            logger.info("INI file not found. \nMake sure a valid 'settings.ini' file exists in the same directory as this script...")
            sys.exit()

        # AGOL Credentials
        inputUsername = General_config.get('AGOL', 'USER')
        inputPswd = General_config.get('AGOL', 'PASS')
        use_prxy = General_config.get('PROXY', 'USEPROXY')
        pxy_srvr = General_config.get('PROXY', 'SERVER')
        pxy_port = General_config.get('PROXY', 'PORT')
        pxy_user = General_config.get('PROXY', 'USER')
        pxy_pass = General_config.get('PROXY', 'PASS')

        proxyDict = {}
        if ast.literal_eval(use_prxy):
            http_proxy = "http://" + pxy_user + ":" + pxy_pass + "@" + pxy_srvr + ":" + pxy_port
            https_proxy = "http://" + pxy_user + ":" + pxy_pass + "@" + pxy_srvr + ":" + pxy_port
            ftp_proxy = "http://" + pxy_user + ":" + pxy_pass + "@" + pxy_srvr + ":" + pxy_port
            proxyDict = {"http": http_proxy, "https": https_proxy, "ftp": ftp_proxy}
        
        # You will need to change this directory to point to your FS_FeatureService.ini files
        iniFileLocation = r"C:\Path\To\FS_INI\Files"

        # This is the portion that actually looks at the .mxd creates the new sd file and uploads the sd file to ArcGIS Online
        # This is also the function from above
        logger.info("Performing the SD file creation and update in ArcGIS Online")
        UpdateServiceDefinitionFile(localPath, inputUsername, inputPswd, proxyDict, iniFileLocation)

        logger.info("Finished...")

# Handle arcpy errors
except arcpy.ExecuteError:
    msgs = arcpy.GetMessages(2)
    logger.exception(msgs)
    raise
except:
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
    msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages(2) + "\n"
    if tbinfo:
        logger.exception(pymsg + "\n")
    if arcpy.GetMessages(2):
        logger.exception(msgs)
    raise
finally:
    end = datetime.datetime.now()
    total = end - start
    days = total.days
    hours = divmod(total.seconds, 3600)
    minutes = divmod(hours[1], 60)
    seconds = minutes[1]
    logger.info('Processing took {0} days, {1} hours, {2} minutes and {3} seconds.'.format(days, hours[0], minutes[0], seconds))
