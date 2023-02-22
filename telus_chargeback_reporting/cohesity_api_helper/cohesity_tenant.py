"""Tenant APIs

    Raises:
        ValueError: Response not a success

    Returns:
        _type_: dict
"""
import requests
from cohesity_api_helper.cohesity_stats import CohesityProtectionStatsObject

class CohesityTenants(CohesityProtectionStatsObject):
    """Cohesity Tenant Class

    Args:
        CohesityProtectionStatsObject (_type_): Cohesity Protection Stats API Class
    """

    def get_tenants(self) -> dict:
        """Get Tenants

        Raises:
            ValueError: Response not a success

        Returns:
            dict: dictonary of tenant objects
        """
        get_tenants_endpoint = "/irisservices/api/v1/public/tenants"
        url = self.protocol + self.cluster_url + get_tenants_endpoint
        tenants = []
        get_tenants_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
        if get_tenants_response.status_code != 200:
            raise ValueError
        for tenant in get_tenants_response.json():
            tenants.append(tenant['tenantId'])
        return tenants
                