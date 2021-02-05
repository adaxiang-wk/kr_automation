import requests 
from msal import ConfidentialClientApplication
#API authentication

#### CONFIGURATION ####
""" The scope(s) the application needs to access on the endpoint """
# scopes = ["api://origination-dcmentryapi-prd/.default"]
# #scopes = ["api://origination-loanentryapi-pie/.default"]# TODO change the VERTICAL and the ENV!
# # ^ in case of app secret auth, this needs to be ".default"


# """ The Id of the azure ad application """
# if scopes == ["api://origination-dcmentryapi-pie/.default"]:
#     clientId = '5400d1f6-0f5e-4fe4-a0b1-8171abf4b86d'
# elif scopes == ["api://origination-dcmentryapi-prd/.default"]:
#     clientId = '41a80cdd-6771-4190-af0f-5671ab236c14'


# """ The application password """
# if scopes == ["api://origination-dcmentryapi-pie/.default"]:
#     clientSecret = 'rg1l@97u8@-Q=iIX.o?xP@sHL/C/u2th' 
# elif scopes == ["api://origination-dcmentryapi-prd/.default"]:
#     clientSecret = 'mOg5lYnTsu0N/U7pnGHOgxwe[ub/1w.B'


""" HTTPS server certificate verification. True/False/File path for certificate """
REQ_VERIFY = './dependencies/DLcert1.cer'
#### END OF CONFIGURATION ####


""" This is the azure ad instance with the name of the tenant """
AUTHORITY = "https://login.microsoftonline.com/DEALOGIC.onmicrosoft.com"


""" This is the authenticated app object, that does the authentication """
class Authenticator():
    def __init__(self, env_name, section):
        self.env_name = env_name
        self.section = section
        self.scopes = [f"api://origination-{self.section}entryapi-{self.env_name}/.default"]
        self.app = self.initialize_auth()


    def get_secret_id(self):
        if self.env_name == 'pie':
            clientId = '5400d1f6-0f5e-4fe4-a0b1-8171abf4b86d'
            clientSecret = 'rg1l@97u8@-Q=iIX.o?xP@sHL/C/u2th'
        else:
            if self.section == 'loan':
                clientId = '41a80cdd-6771-4190-af0f-5671ab236c14'
                clientSecret = 'mOg5lYnTsu0N/U7pnGHOgxwe[ub/1w.B'
            elif self.section == 'dcm':
                clientId = 'f688fdcb-671a-406d-b3f8-9e6dbeff84d6'
                clientSecret = 'ENeZ2Q6e9-.LY6C~81ixIpT881Id_TV1RI'
            else:
                clientId = 'f688fdcb-671a-406d-b3f8-9e6dbeff84d6'
                clientSecret = 'ENeZ2Q6e9-.LY6C~81ixIpT881Id_TV1RI'

        return clientId, clientSecret


    def get(self, url, getToken = True):
        """
        Wrapper for the requests.get() - Calls the url endpoint with the supplied authentication token and returns the result


        Args:
            url (str): the url to call
            token (str): the bearer token to use
        """
        token = None
        if getToken:
            token = self.get_access_token()
            pass
        response = requests.get(url, verify=REQ_VERIFY, headers=self.assemble_auth_header(token))
        return response


    def put(self, url, data, getToken = True):
        """
        Wrapper for the requests.put() - Calls the url endpoint with the supplied authentication token and returns the result


        Args:
            url (str): the url to call
            data (object): the data to send to the server
            token (str): the bearer token to use
        """
        token = None
        if getToken:
            token = self.get_access_token()
            pass
        headers = self.assemble_auth_header(token)
        headers["Content-Type"] = "application/json"
        response = requests.put(url, verify=REQ_VERIFY, headers=headers, data=data)
        return response


    def post(self, url, data, getToken = True):
        """
        Wrapper for the requests.post() - Calls the url endpoint with the supplied authentication token and returns the result


        Args:
            url (str): the url to call
            data (object): the data to send to the server
            token (str): the bearer token to use
        """
        token = None
        if getToken:
            token = self.get_access_token()
            pass
        headers = self.assemble_auth_header(token)
        headers["Content-Type"] = "application/json"
        response = requests.post(url, verify=REQ_VERIFY, headers=headers, data=data)
        return response


    def assemble_auth_header(self, token):
        headers = None
        if token:
            headers = { 'Authorization': 'Bearer ' + token}
            pass
        return headers


    def initialize_auth(self):
        """
        Initializes the authentication app based on the id and the client credentials against the authority


        Args:
            client_id (str): id of the app (registered in the azure ad)
            credentials (str): the client secret of the app
        """
        clientId, clientSecret = self.get_secret_id()

        authapp = ConfidentialClientApplication(client_id=clientId
            , authority=AUTHORITY
            , client_credential=clientSecret)
        return authapp


    def get_token(self):
        """
        Tries to get the token from the auth provider server
        """

        result = None
        # try to get the token fromt the cache
        result = self.app.acquire_token_silent(self.scopes, account=None)
        if not result:
            # get a new token
            result = self.app.acquire_token_for_client(scopes=self.scopes)
        if "error" in result:
            raise Exception(f'Could not get token!!! {result["error_description"]}')


        return result


    def get_access_token(self):
        """
        Gets the access token from the auth provider
        """

        token = None
        token = self.get_token()
        if not token:
            return None

        return token["access_token"]

#app = initialize_auth(clientId, clientSecret)

if __name__ == "__main__":
    # app = initialize_auth(scope='pie')
    # print(app)
    my_auth = Authenticator(env_name='pie', section='dcm')
    print(my_auth.app)
