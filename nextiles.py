
import json
import os
import sys, getopt
import urllib.request

from datetime import datetime, timedelta
from typing import List


URL = 'https://68a9g89ek2.execute-api.us-east-1.amazonaws.com/alpha/trainingdata/?'
LS_URL = 'https://68a9g89ek2.execute-api.us-east-1.amazonaws.com/alpha/trainingdata/?listfiles=True&'
DIRECTORY = 'data'

class RequestModel:
    def __init__(self,username,organization,token,start_time,end_time):
        self.username = username
        self.organization = organization
        self.token = token
        self.start_time = start_time
        self.end_time = end_time


def return_url(request_obj:RequestModel,url:str)->str:
    
    # Because backend expects this format without white spaces
    if request_obj.start_time is not None:
        start_time=datetime.strftime(request_obj.start_time,"%Y%m%d%H%M%S")
    else:
        now = datetime.now()
        start_time = now.strftime("%Y%m%d%H%M%S")
    if request_obj.end_time == None:
        end_time = datetime.strptime(start_time,"%Y%m%d%H%M%S")+timedelta(days=1)
        # REFORMAT
        end_time = datetime.strftime(end_time,"%Y%m%d%H%M%S")
        return f"{url}username={request_obj.username}&organization={request_obj.organization}&start_range={start_time}&end_range={end_time}"
        # when user has passed the endtime date
    elif request_obj.end_time is not None:
        end_time = datetime.strftime(request_obj.end_time,"%Y%m%d%H%M%S")
        return f"{url}username={request_obj.username}&organization={request_obj.organization}&start_range={start_time}&end_range={end_time}"

def get_data(request_object,url):
    request = urllib.request.Request(url=url,headers={'x-api-key':request_object.token})
    response = None
    try:
        with urllib.request.urlopen(request) as f:
            response = f.read().decode('utf-8')
        response=json.loads(response)
    except urllib.error.HTTPError as e:
        if (e.status==502):
            print("ERROR: Too much data, pass me a shorter timeperiod")
        else:
            print(f"ERROR: {e}")
        sys.exit(2)
    return response

def get_list_count(request_object):
    _url=return_url(request_obj=request_object,url=LS_URL)
    response=get_data(request_object=request_object,url=_url)
    if response is None:
        print("Nothing received")
        sys.exit(2)
    print(response)

def store_response_directory(response,request_object):
    # First create the Organization directory
    _org_directory=os.path.join(DIRECTORY,request_object.organization)
    if not os.path.exists(_org_directory):
        os.makedirs(_org_directory)
    
    # Second create the User directory
    _user_dir=os.path.join(_org_directory,request_object.username)
    if not os.path.exists(_user_dir):
        os.makedirs(_user_dir)
    _path = _user_dir

    # Now Saving the files in the directory
    for _date in response:
        _path_dir = os.path.join(_path, _date)
        if not os.path.exists(_path_dir):
            os.makedirs(_path_dir)
        for _time in response[_date]:
            _path_time = os.path.join(_path_dir,_time.replace(":","-"))
            if not os.path.exists(_path_time):
                os.makedirs(_path_time)
            for file_name in response[_date][_time]:
                _file_name_path = os.path.join(_path_time,file_name.replace(":","-"))
                with open(f'{_file_name_path}.csv','w') as f:
                    f.write(str(response[_date][_time][file_name]))
    print(f"Downloaded!, check /{DIRECTORY} directory")
                


def main(argv):
   request_object = parse_arguments(argv)
   _url=return_url(request_obj=request_object,url=URL)
   response=get_data(request_object=request_object,url=_url)
   if response is None:
       print("Nothing received")
       sys.exit(2)
   store_response_directory(response,request_object=request_object)
   
   

def parse_arguments(argv:List[str]) -> RequestModel:
    username = None
    organization = None
    token = None
    start_time = None
    end_time = None
    _list = False
    try:
      opts, args = getopt.getopt(argv,"hu:o:t:s:e:",["username=","organization=","token=","start_timestamp=","end_timestamp="])
    #   print(opts, args)
      if "list" in args:
          _list = True
    except getopt.GetoptError:
        print('ERROR: Execute the script as: \n python nextiles.py -u <username> -o <organization> -t <token> -s <start_timestamp> -e <end_timestamp>')
        print('\nExample:\n\t python3 nextiles.py -u Siddharth -o Nextiles -t your-token -s 2021-06-16 -e 2021-06-22')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('\nExample:\n\t python3 nextiles.py -u Siddharth -o Nextiles -t your-token -s 2021-06-16 -e 2021-06-22')
            sys.exit()
        elif opt in ("-u", "--username"):
            username = arg
        elif opt in ("-o", "--organization"):
            organization = arg
        elif opt in ("-t","--token"):
            token = arg
            if len(token)>60:
                print('ERROR: Invalid Token length',len(token))
                sys.exit(2)
        elif opt in ("-s","--start_timestamp"):
            start_time = arg
            try:
                start_time=datetime.strptime(start_time,"%Y-%m-%d")
            except Exception as e:
                print('ERROR: start_time should be YYYY-mm-dd format, for ex: 2021-08-10')
                sys.exit(2)
        elif opt in ("-e","--end_timestamp"):
            end_time = arg
            try:
                # If no end_time is passed then only check if it's the right format and convert it to a datetime function
                if end_time is not None:
                    end_time = datetime.strptime(end_time,"%Y-%m-%d")
            except Exception as e:
                print("ERROR: end_time should be YYYY-mm-dd format, for ex: 2021-08-11'")
                sys.exit(2)
    
    # checks if all the required parameters are passed
    if None in [organization,username,token,start_time] and _list!=True:
        print('Error: username, organization, token, and start_time; All of these are required')
        sys.exit(2)
    elif None in [organization,username,token] and _list==True:
        print('Error: username, organization, token is required for Listing all the data')
        sys.exit(2)

    if _list:
        get_list_count(RequestModel(username,organization,token,None,None))
        sys.exit(2)

    return RequestModel(username,organization,token,start_time,end_time)

# EntryPoint for the script
if __name__ == "__main__":
   main(sys.argv[1:])