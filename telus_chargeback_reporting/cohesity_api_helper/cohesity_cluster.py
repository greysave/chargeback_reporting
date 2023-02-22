"""Cluster Info Module"""
import requests
from cohesity_api_helper.cohesity_stats import CohesityProtectionStatsObject

class CohesityClusterobject(CohesityProtectionStatsObject):
    """Cohesity Cluster Class

    Args:
        CohesityProtectionStatsObject (CohesityProtectionStatsObject): Class to represent the Cohesity cluster object
    """

    def get_cluster_info(self) -> dict:
        """Get Cluster Info

        Raises:
            ValueError: If status_code does not return success

        Returns:
            dict: Cluster Object
        """
        get_cluster_endpoint = "/irisservices/api/v1/public/cluster"
        url = self.protocol + self.cluster_url + get_cluster_endpoint
        cluster_response = requests.get(url = url, headers = self.headers, verify=False, timeout = 60)
        if cluster_response.status_code != 200:
            raise ValueError
        self.api_payload['cluster'] = cluster_response.json()
 