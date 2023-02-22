"""Module to manipulate Cohesity time series stats:w

    Raises:
        ValueError: Response not a success
        ValueError: Response not a success
        ValueError: Response not a success
        ValueError: Response not a success
        ValueError: Response not a success
        ValueError: Response not a success
        ValueError: Response not a success
        ValueError: Response not a success
        ValueError: Response not a success
        ValueError: Response not a success
        ValueError: Response not a success

    """
import datetime
import requests
import pandas as pd
from requests.structures import CaseInsensitiveDict

class CohesityProtectionStatsObject:
    """Cohesity Protection Stats Object
    """
    def __init__(self, bearer_token, cluster_url):
        #Class Variables
        json_type = "application/json"
        #Class Attributes
        # cohesity_auth = CohesityUserAuthentication()
        self.cluster_url = cluster_url
        self.bearer_token = bearer_token
        #Declare protection jobs object
        self.protocol = "https://"
        self.headers = CaseInsensitiveDict()
        self.headers['Accept'] = json_type
        self.headers['Content-type'] = json_type
        self.headers['Authorization'] = 'bearer '+ self.bearer_token
        self.api_payload = {}
        self.start_time = None
        self.end_time = None
        self.df_warm = None
        self.df_sum = None
        self.df_replica = None
        self.df_replica_sum = None
        self.df_view = None
        self.df_view_sum = None
        self.df_vault = None
        self.df_vault_sum = None

    def convert_epoch_usecs(self, epoch: float) -> datetime:
        """Convert epoch time from microseconds to standard time

        Args:
            epoch (float): epoch time

        Returns:
            datetime: standard time
        """
        return datetime.datetime.fromtimestamp((epoch)/10**6)

    def convert_epoch_msecs(self, epoch: float) -> datetime:
        """convert epoch time from milliseconds to standard time

        Args:
            epoch (float): epoch time

        Returns:
            datetime: standard time
        """
        return  datetime.datetime.fromtimestamp(epoch/10**3)

    def get_protection_jobs(self) -> None:
        """Get warm protection jobs

        Raises:
            ValueError: Response not a success
        """
        self.api_payload['tenants'] = {}
        for tenant in self.api_payload['tenantIds']:
            get_protection_job_endpoint = f'/irisservices/api/v1/public/stats/consumers?organizationsIdList={tenant}&msecsBeforeCurrentTimeToCompare=604800000&maxCount=1000&fetchViewBoxName=true&fetchTenantName=true&fetchProtectionEnvironment=true&consumerType=kProtectionRuns'
            url = self.protocol + self.cluster_url + get_protection_job_endpoint
            storage_consumers_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
            self.api_payload['tenants'].update({tenant: {}})
            if storage_consumers_response.status_code != 200:
                raise ValueError
            self.api_payload['tenants'][tenant]['stats_list'] = storage_consumers_response.json()['statsList']

    def get_replica_protection_jobs(self) -> None:
        """Get replica protection objects

        Raises:
            ValueError: Response not a success
        """
        for tenant in self.api_payload['tenantIds']:
            get_replica_protection_job_endpoint = f'/irisservices/api/v1/public/stats/consumers?organizationsIdList={tenant}&msecsBeforeCurrentTimeToCompare=604800000&maxCount=1000&fetchViewBoxName=true&fetchTenantName=true&fetchProtectionEnvironment=true&consumerType=kReplicationRuns'
            url = self.protocol + self.cluster_url + get_replica_protection_job_endpoint
            replica_storage_consumers_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
            if replica_storage_consumers_response.status_code != 200:
                raise ValueError
            self.api_payload['tenants'][tenant]['replica_stats_list'] = replica_storage_consumers_response.json()['statsList']

    def view_storage_info(self) -> None:
        """Get Cold Storage Info

        Raises:
            ValueError: Response not a success
        """
        for tenant in self.api_payload['tenantIds']:
            get_replica_protection_job_endpoint = f'/irisservices/api/v1/public/stats/consumers?organizationsIdList={tenant}&msecsBeforeCurrentTimeToCompare=604800000&maxCount=1000&fetchViewBoxName=true&fetchTenantName=true&fetchProtectionPolicy=true&fetchProtectionEnvironment=true&consumerType=kViews'
            url = self.protocol + self.cluster_url + get_replica_protection_job_endpoint
            replica_storage_consumers_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
            if replica_storage_consumers_response.status_code != 200:
                raise ValueError
            self.api_payload['tenants'][tenant]['view_stats_list'] = replica_storage_consumers_response.json()['statsList']

    def get_storage_consumed(self) -> None:
        """Get warm storage consumed

        Raises:
            ValueError: Response not a success
        """
        tenant_list = []
        tenant_id_list = []
        job_name_list = []
        storage_consumed_list = []
        time_list = []
        for stats in self.api_payload['tenants'].values():
            for stat in stats['stats_list']:
                for entity in stat['groupList']:
                    entity_id = entity['entityId']
                    get_storage_consumed_endpoint = f'/irisservices/api/v1/public/statistics/timeSeriesStats?startTimeMsecs={self.start_time}&schemaName=BookKeeperStats&metricName=ChunkBytesPhysical&rollupIntervalSecs=86400&rollupFunction=latest&entityIdList={entity_id}&endTimeMsecs={self.end_time}'
                    url = self.protocol + self.cluster_url + get_storage_consumed_endpoint
                    storage_consumed_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
                    if storage_consumed_response.status_code != 200:
                        raise ValueError
                    for consumed in storage_consumed_response.json()['dataPointVec']:
                        tenant_list.append(entity['tenantName'])
                        tenant_id_list.append(entity['tenantId'])
                        time_list.append(consumed['timestampMsecs'])
                        job_name_list.append(stat['name'])
                        storage_consumed_list.append(consumed['data']['int64Value'])
        self.df_warm = pd.DataFrame({'Tenant Name': tenant_list, 'Tenant ID': tenant_id_list, 'Job Name': job_name_list, 'Time': time_list, 'Warm Storage Consumed': storage_consumed_list})
        df_sorted = self.df_warm.sort_values(['Tenant Name', 'Tenant ID', 'Time', 'Job Name'], ascending = [True, True, True, True], inplace = False)
        df_dropped = df_sorted.drop_duplicates(subset='Job Name', keep='last')
        self.df_sum = df_dropped.groupby(['Tenant Name'])['Warm Storage Consumed'].sum().reset_index()

    def get_data_written(self) -> None:
        """Warm Get Written Data

        Raises:
            ValueError: Response not a success
        """
        tenant_list = []
        time_list = []
        job_name_list = []
        data_written_list = []
        tenant_id_list = []
        for  stats in self.api_payload['tenants'].values():
            for stat in stats['stats_list']:
                for entity in stat['groupList']:
                    entity_id = entity['entityId']
                    get_data_written_endpoint = f'/irisservices/api/v1/public/statistics/timeSeriesStats?startTimeMsecs={self.start_time}&schemaName=BookKeeperStats&metricName=ChunkBytesMorphed&rollupIntervalSecs=86400&rollupFunction=latest&entityIdList={entity_id}&endTimeMsecs={self.end_time}'
                    url = self.protocol + self.cluster_url + get_data_written_endpoint
                    data_written_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
                    if data_written_response.status_code != 200:
                        raise ValueError
                    for written in data_written_response.json()['dataPointVec']:
                        tenant_id_list.append(entity['tenantId'])
                        tenant_list.append(entity['tenantName'])
                        time_list.append(written['timestampMsecs'])
                        job_name_list.append(stat['name'])
                        data_written_list.append(written['data']['int64Value'])
        df_written =  pd.DataFrame({'Tenant Name': tenant_list, 'Tenant ID': tenant_id_list, 'Job Name': job_name_list, 'Time': time_list, 'Warm Data Written': data_written_list})
        df_sorted = df_written.sort_values(['Tenant Name', 'Tenant ID', 'Time', 'Job Name'], ascending = [True, True, True, True], inplace = False)
        df_dropped  = df_sorted.drop(['Tenant Name', 'Tenant ID', 'Time', 'Job Name'], axis = 1, inplace = False)
        self.df_warm = self.df_warm.join(df_dropped)
        df_drop_dup = df_sorted.drop_duplicates(subset='Job Name', keep='last')
        df_dropped_sum = df_drop_dup.groupby(['Tenant Name'])['Warm Data Written'].sum().reset_index()
        df_dropped_sum.drop(['Tenant Name'], axis = 1, inplace = True)
        self.df_sum = self.df_sum.join(df_dropped_sum)

    def get_logical_usage(self) -> None:
        """Get Warm Logical Usage

        Raises:
            ValueError: Response not a success
        """
        logical_usage = []
        job_name_list = []
        time_list = []
        tenant_list = []
        tenant_id_list = []
        for stats in self.api_payload['tenants'].values():
            for stat in stats['stats_list']:
                for entity in stat['groupList']:
                    entity_id = entity['entityId']
                    get_logical_usage_endpoint = f'/irisservices/api/v1/public/statistics/timeSeriesStats?startTimeMsecs={self.start_time}&schemaName=BookKeeperStats&metricName=LogicalUsage&rollupIntervalSecs=86400&rollupFunction=latest&entityIdList={entity_id}&endTimeMsecs={self.end_time}'
                    url = self.protocol + self.cluster_url + get_logical_usage_endpoint
                    logical_usage_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
                    if logical_usage_response.status_code != 200:
                        raise ValueError
                    for usage in logical_usage_response.json()['dataPointVec']:
                        tenant_id_list.append(entity['tenantId'])
                        tenant_list.append(entity['tenantName'])
                        time_list.append(usage['timestampMsecs'])
                        job_name_list.append(stat['name'])
                        logical_usage.append(usage['data']['int64Value'])
        df_logical =  pd.DataFrame({'Tenant Name': tenant_list, 'Tenant ID': tenant_id_list, 'Job Name': job_name_list, 'Time': time_list, 'Warm Logical Usage': logical_usage})
        df_sorted = df_logical.sort_values(['Tenant Name', 'Tenant ID', 'Time', 'Job Name'], ascending = [True, True, True, True], inplace = False)
        df_drop_dup = df_sorted.drop_duplicates(subset='Job Name', keep='last')
        df_dropped_sum = df_drop_dup.groupby(['Tenant Name'])['Warm Logical Usage'].sum().reset_index()
        df_dropped_sum.drop(['Tenant Name'], axis = 1, inplace = True)
        self.df_sum = self.df_sum.join(df_dropped_sum)

    def get_replica_storage_consumed(self) -> None:
        """Get Replica Storage Consumed

        Raises:
            ValueError: Response not a success
        """
        tenant_list = []
        tenant_id_list = []
        job_name_list = []
        storage_consumed_list = []
        time_list = []
        for stats in self.api_payload['tenants'].values():
            for stat in stats['replica_stats_list']:
                for entity in stat['groupList']:
                    entity_id = entity['entityId']
                    get_replica_storage_consumed_endpoint = f'/irisservices/api/v1/public/statistics/timeSeriesStats?startTimeMsecs={self.start_time}&schemaName=BookKeeperStats&metricName=ChunkBytesMorphed&rollupIntervalSecs=86400&rollupFunction=latest&entityIdList={entity_id}&endTimeMsecs={self.end_time}'
                    url = self.protocol + self.cluster_url + get_replica_storage_consumed_endpoint
                    replica_storage_consumed_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
                    if replica_storage_consumed_response.status_code != 200:
                        raise ValueError
                    for consumed in replica_storage_consumed_response.json()['dataPointVec']:
                        tenant_list.append(entity['tenantName'])
                        tenant_id_list.append(entity['tenantId'])
                        time_list.append(consumed['timestampMsecs'])
                        storage_consumed_list.append(consumed['data']['int64Value'])
                        job_name_list.append(stat['name'])
        self.df_replica = pd.DataFrame({'Tenant Name': tenant_list, 'Tenant ID': tenant_id_list, 'Job Name': job_name_list,'Time': time_list,  'Replica Storage Consumed': storage_consumed_list})
        df_sorted = self.df_replica.sort_values(['Tenant Name', 'Tenant ID', 'Time', 'Job Name'], ascending = [True, True, True, True], inplace = False)
        df_dropped = df_sorted.drop_duplicates(subset='Job Name', keep='last')
        self.df_replica_sum = df_dropped.groupby(['Tenant Name'])['Replica Storage Consumed'].sum().reset_index()

    def get_replica_data_written(self) -> None:
        """Get Replica Data Written

        Raises:
            ValueError: Response not a success
        """
        data_written_list = []
        job_name_list = []
        time_list = []
        tenant_list = []
        tenant_id_list = []
        for tenant in self.api_payload['tenants'].values():
            for stat in tenant['replica_stats_list']:
                for entity in stat['groupList']:
                    entity_id = entity['entityId']
                    get_data_written_endpoint = f'/irisservices/api/v1/public/statistics/timeSeriesStats?startTimeMsecs={self.start_time}&schemaName=BookKeeperStats&metricName=ChunkBytesMorphed&rollupIntervalSecs=86400&rollupFunction=latest&entityIdList={entity_id}&endTimeMsecs={self.end_time}'
                    url = self.protocol + self.cluster_url + get_data_written_endpoint
                    replica_data_written_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
                    if replica_data_written_response.status_code != 200:
                        raise ValueError
                    for written in replica_data_written_response.json()['dataPointVec']:
                        tenant_id_list.append(entity['tenantId'])
                        tenant_list.append(entity['tenantName'])
                        time_list.append(written['timestampMsecs'])
                        job_name_list.append(stat['name'])
                        data_written_list.append(written['data']['int64Value'])
        df_written =  pd.DataFrame({'Tenant Name': tenant_list, 'Tenant ID': tenant_id_list, 'Job Name': job_name_list, 'Time': time_list, 'Replica Data Written': data_written_list})
        df_sorted = df_written.sort_values(['Tenant Name', 'Tenant ID', 'Time', 'Job Name'], ascending = [True, True, True, True], inplace = False)
        df_dropped  = df_sorted.drop(['Tenant Name', 'Tenant ID', 'Time', 'Job Name'], axis = 1, inplace = False)
        self.df_replica = self.df_replica.join(df_dropped)
        df_drop_dup = df_sorted.drop_duplicates(subset='Job Name', keep='last')
        df_dropped_sum = df_drop_dup.groupby(['Tenant Name'])['Replica Data Written'].sum().reset_index()
        df_dropped_sum.drop(['Tenant Name'], axis = 1, inplace = True)
        self.df_replica_sum = self.df_replica_sum.join(df_dropped_sum)

    def get_view_storage_consumed(self) -> None:
        """Get Cold Storage Consumed

        Raises:
            ValueError: Response not a success
        """
        tenant_list = []
        job_name_list = []
        storage_consumed_list = []
        time_list = []
        tenant_id_list = []
        storage_consumed_list = []
        for stats in self.api_payload['tenants'].values():
            for stat in stats['view_stats_list']:
                for entity in stat['groupList']:
                    entity_id = entity['entityId']
                    get_view_storage_consumed_endpoint = f'/irisservices/api/v1/public/statistics/timeSeriesStats?startTimeMsecs={self.start_time}&schemaName=BookKeeperStats&metricName=ChunkBytesPhysical&rollupIntervalSecs=86400&rollupFunction=latest&entityIdList={entity_id}&endTimeMsecs={self.end_time}'
                    url = self.protocol + self.cluster_url + get_view_storage_consumed_endpoint
                    view_storage_consumed_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
                    if view_storage_consumed_response.status_code != 200:
                        raise ValueError
                    for consumed in view_storage_consumed_response.json()['dataPointVec']:
                        tenant_list.append(entity['tenantName'])
                        tenant_id_list.append(entity['tenantId'])
                        time_list.append(consumed['timestampMsecs'])
                        storage_consumed_list.append(consumed['data']['int64Value'])
                        job_name_list.append(stat['name'])
        self.df_view = pd.DataFrame({'Tenant Name': tenant_list, 'Job Name': job_name_list, 'Tenant ID': tenant_id_list, 'Time': time_list, 'Cold Storage Consumed': storage_consumed_list})
        df_sorted = self.df_view.sort_values(['Tenant Name', 'Tenant ID', 'Time', 'Job Name'], ascending = [True, True, True, True], inplace = False)
        df_dropped = df_sorted.drop_duplicates(subset='Job Name', keep='last')
        self.df_view_sum = df_dropped.groupby(['Tenant Name'])['Cold Storage Consumed'].sum().reset_index()

    def get_view_data_written(self) -> None:
        """Cold Data Written

        Raises:
            ValueError: Response not a success
        """
        data_written_list = []
        job_name_list = []
        time_list = []
        tenant_list = []
        tenant_id_list = []
        for tenant in self.api_payload['tenants'].values():
            for stat in tenant['view_stats_list']:
                for entity in stat['groupList']:
                    entity_id = entity['entityId']
                    get_data_written_endpoint = f'/irisservices/api/v1/public/statistics/timeSeriesStats?startTimeMsecs={self.start_time}&schemaName=BookKeeperStats&metricName=ChunkBytesMorphed&rollupIntervalSecs=86400&rollupFunction=latest&entityIdList={entity_id}&endTimeMsecs={self.end_time}'
                    url = self.protocol + self.cluster_url + get_data_written_endpoint
                    view_data_written_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
                    if view_data_written_response.status_code != 200:
                        raise ValueError
                    for written in view_data_written_response.json()['dataPointVec']:
                        tenant_id_list.append(entity['tenantId'])
                        tenant_list.append(entity['tenantName'])
                        time_list.append(written['timestampMsecs'])
                        job_name_list.append(stat['name'])
                        data_written_list.append(written['data']['int64Value'])
        df_written =  pd.DataFrame({'Tenant Name': tenant_list, 'Tenant ID': tenant_id_list, 'Job Name': job_name_list, 'Time': time_list, 'Cold Data Written': data_written_list})
        df_sorted = df_written.sort_values(['Tenant Name', 'Tenant ID', 'Time', 'Job Name'], ascending = [True, True, True, True], inplace = False)
        df_dropped  = df_sorted.drop(['Tenant Name', 'Tenant ID', 'Time', 'Job Name'], axis = 1, inplace = False)
        self.df_view = self.df_view.join(df_dropped)
        df_drop_dup = df_sorted.drop_duplicates(subset='Job Name', keep='last')
        df_dropped_sum = df_drop_dup.groupby(['Tenant Name'])['Cold Data Written'].sum().reset_index()
        df_dropped_sum.drop(['Tenant Name'], axis = 1, inplace = True)
        self.df_view_sum = self.df_view_sum.join(df_dropped_sum)

    def get_vault_consumed(self) -> None:
        """Fronzen Consumed

        Raises:
            ValueError: Response not a success
        """
        name_list = []
        time_list = []
        type_list = []
        bucket_name_list = []
        vault_consumed_list = []
        for vault in self.api_payload['vaults']:
            vault_id = vault['id']
            get_vault_data_endpoint =  f'/irisservices/api/v1/public/statistics/timeSeriesStats?endTimeMsecs={self.end_time}&entityId={vault_id}&metricName=kMorphedUsageBytes&metricUnitType=0&range=month&rollupFunction=latest&rollupIntervalSecs=86400&schemaName=kIceboxVaultStats&startTimeMsecs={self.start_time}'
            url = self.protocol + self.cluster_url + get_vault_data_endpoint
            vault_data_response = requests.get(url = url, headers = self.headers, verify = False, timeout = 60)
            for key, item in vault['config'].items():
                if vault_data_response.status_code != 200:
                    raise ValueError
                for consumed in vault_data_response.json()['dataPointVec']:
                    time_list.append(consumed['timestampMsecs'])
                    vault_consumed_list.append(consumed['data']['int64Value'])
                    if key == "bucketName":
                        bucket_name_list.append(item)
                    name_list.append(vault['name'])
                    type_list.append(vault['externalTargetType'])
        self.df_vault = pd.DataFrame({'Archive Name': name_list, 'Archive Provider': type_list, 'Time': time_list,
                           'Vault Consumed': vault_consumed_list})
        df_sorted = self.df_vault.sort_values(['Archive Name', 'Time'], ascending = [True, True], inplace = False)
        df_dropped = df_sorted.drop_duplicates(subset='Archive Name', keep='last')
        self.df_vault_sum = df_dropped.groupby(['Archive Name'])['Vault Consumed'].sum().reset_index()
