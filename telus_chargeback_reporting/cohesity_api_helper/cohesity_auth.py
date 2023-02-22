"""Authentication Module"""
import json
import dataclasses
import requests
from requests.structures import CaseInsensitiveDict
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

@dataclasses.dataclass
class CohesityUserAuthentication:
    """Cohesity Authentication Class
    """
    def __init__(self, cluster_url):
        #Declare Class Variables
        json_type = "application/json"
        #Declare Attributes
        self.cluster_url = str(cluster_url)
        self.protocol = "https://"
        self.headers = CaseInsensitiveDict()
        self.headers['Accept'] = json_type
        self.headers['Content-type']=json_type

    def get_bearer_token(self, username: str, security: str, domain: str) -> str:
        """_summary_

        Args:
            username (str): Cohesity Username
            security (str): Cohesity Security Credential
            domain (str): Cohesity Domain

        Raises:
            ValueError: If status code is not success

        Returns:
            str: Bearer Token
        """
        disable_warnings(category=InsecureRequestWarning)
        post_auth_rest_endpoint = "/irisservices/api/v1/public/accessTokens"
        url = self.protocol + self.cluster_url + post_auth_rest_endpoint
        payload = json.dumps({"username": username, "password": security, "domain": domain })
        bearer_token_response = requests.post(url = url, data=payload, headers = self.headers, verify=False, timeout = 60)
        if bearer_token_response.status_code != 201:
            raise ValueError
        return bearer_token_response.json()['accessToken']
    