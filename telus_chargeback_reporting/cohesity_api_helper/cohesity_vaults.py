"""Frozen Stats Module

    Raises:
        ValueError: Response not a success

    Returns:
        _type_: dict
    """
import requests
from cohesity_api_helper.cohesity_stats import CohesityProtectionStatsObject

class CohesityProtectionVaultObject(CohesityProtectionStatsObject):
    """Get Frozen Objects

    Args:
        CohesityProtectionStatsObject (_type_): Cohesity Protection Stats APIs
    """
    def get_vaults(self) -> dict:
        """Get vault objects

        Raises:
            ValueError: Response not a success

        Returns:
            dict: Vault objects
        """
        get_vault_endpoint = "/irisservices/api/v1/public/vaults"
        url = url = self.protocol + self.cluster_url + get_vault_endpoint
        get_vault_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
        if get_vault_response.status_code != 200:
            raise ValueError
        return get_vault_response.json()
