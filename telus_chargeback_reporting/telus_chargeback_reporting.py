"""Chargeback Main Pplicatoin
"""
import sys
from datetime import datetime
from pip._vendor.requests import exceptions as reqex
from cohesity_api_helper.cohesity_auth import CohesityUserAuthentication
from cohesity_api_helper.cohesity_stats import CohesityProtectionStatsObject
from cohesity_api_helper.cohesity_cluster import CohesityClusterobject
from cohesity_api_helper.csv_export import CSVData
from cohesity_api_helper.cohesity_tenant import CohesityTenants
from cohesity_api_helper.cohesity_vaults import CohesityProtectionVaultObject


def main():
    '''Main function to invoke reporting methods'''
    try:
        cohesity_client_object = CohesityUserAuthentication("localhost:46641")
        cohesity_client = cohesity_client_object.get_bearer_token("cohesity_ui_support_4605337938479312",
                                                                  "Ouqsbq+xDVn3abNIIaoPGk0LH05quoOgkzyjYh/2NmY=", "local")
    except ValueError:
        print("The username or password you entered is incorrect")
    except reqex.ConnectionError as re_error:
        print(re_error, "***Halting can't access the cluster URL***")
        sys.exit()
    try:
        pg_object = CohesityProtectionStatsObject(cohesity_client, cohesity_client_object.cluster_url)
    except ValueError as ve_error:
        print(ve_error)
    except UnboundLocalError as ue_error:
        print(ue_error, "***HALTING Protection stats object***")
        sys.exit()

    try:
        org_object = CohesityTenants(cohesity_client, cohesity_client_object.cluster_url)
        pg_object.api_payload['tenantIds'] = org_object.get_tenants()
    except ValueError as ve_error:
        print(ve_error, "***HALTING Tenants Object***")
        sys.exit()
    except UnboundLocalError as ue_error:
        print(ue_error, "***HALTING Tenants Object***")
        sys.exit()

    try:
        vault_object = CohesityProtectionVaultObject(cohesity_client, cohesity_client_object.cluster_url)
        pg_object.api_payload['vaults'] = vault_object.get_vaults()
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot create frozen object!***")
        sys.exit()
    except KeyError as ke_error:
        print(ke_error, "***Halting cannot create frozen object!***")
        sys.exit()

    try:
        pg_object.get_protection_jobs()
        pg_object.get_replica_protection_jobs()
        pg_object.view_storage_info()
    except ValueError as ve_error:
        print(ve_error, "***HALTING*** Halting Entity Objects***")
        sys.exit()
    except(UnboundLocalError) as ue_error:
        print(ue_error, "***HALTING*** Halting Entity Objects***")
        sys.exit()

    try:
        cl_object = CohesityClusterobject(cohesity_client, cohesity_client_object.cluster_url)
        cl_object.get_cluster_info()
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot create cluster object!!!***")
        sys.exit()
    except KeyError as ke_error:
        print(ke_error, "***Halting cannot create cluster object!!!***")
        sys.exit()

    try:
        pg_object.api_payload['cluster'] = cl_object.api_payload['cluster']
    except ValueError as ve_error:
        print(ve_error, "***Halting missing cluster object***")
        sys.exit()
    except KeyError as ke_error:
        print(ke_error, "***Halting missing cluster object***")
        sys.exit()

    try:
        pg_object.start_time = pg_object.api_payload['cluster']['currentTimeMsecs'] - 2419200000
        pg_object.end_time = pg_object.api_payload['cluster']['currentTimeMsecs']
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot calculate start time!***")
        sys.exit()
    except KeyError as ke_error:
        print(ke_error, "***Halting cannot calculate start time!***")
        sys.exit()

    try:
        pg_object.get_storage_consumed()
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot get the cold storage consumed!***")
        sys.exit()

    try:
        pg_object.get_data_written()
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot get the warm data written!***")
        sys.exit()

    try:
        pg_object.get_logical_usage()
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot get the warm logical usage!***")
        sys.exit()

    try:
        pg_object.get_replica_storage_consumed()
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot get the replica storage consumed!***")
        sys.exit()

    try:
        pg_object.get_replica_data_written()
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot get the replica data written!***")
        sys.exit()

    try:
        pg_object.get_view_storage_consumed()
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot get the cold storage consumed!***")
        sys.exit()

    try:
        pg_object.get_view_data_written()
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot get the cold data written!***")
        sys.exit()

    try:
        pg_object.get_vault_consumed()
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot get the frozen storage consumed!***")
        sys.exit()

    try:
        csv_data = CSVData(pg_object.df_warm, pg_object.df_sum, pg_object.df_replica,
                           pg_object.df_replica_sum, pg_object.df_view, pg_object.df_view_sum,
                           pg_object.df_vault, pg_object.df_vault_sum)
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot make export object!***")
        sys.exit()
    except KeyError as ke_error:
        print(ke_error, "***Halting cannot make export object!***")
        sys.exit()

    current_date_time=((datetime.now()).strftime("%Y%m%d_%H%M%S"))

    try:
        csv_data_reportname=pg_object.api_payload['cluster']['name'] + "_" + current_date_time + ".xlsx"
        csv_data.save_as_csv(csv_data_reportname)
    except ValueError as ve_error:
        print(ve_error, "***Halting cannot create report file!***")
        sys.exit()
    except PermissionError as pe_error:
        print(pe_error, "***Halting unable to write the file.  Application directory does not have_error the correct permissions***")
        sys.exit()

if __name__ == '__main__':
    main()
    