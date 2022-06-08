# CHANGE LINE 78 TO NEW PATH

import sys
import mysql.connector
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


def main(client):
    ga_service = client.get_service("GoogleAdsService")
    # GAQL Query
    query = """
            SELECT 
            campaign.id,
            campaign.name,
            ad_group.campaign,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_per_conversion,
            metrics.ctr,
            metrics.average_cpc,
            metrics.average_cpm,
            metrics.all_conversions_from_interactions_rate,
            metrics.conversions_value,
            metrics.search_impression_share,
            metrics.conversions
            FROM ad_group
            WHERE
            segments.date DURING YESTERDAY
            """

    # Issues a search request using streaming.
    response = ga_service.search_stream(
        customer_id=str(9653524575119), query=query)

    # Init connection to mySQL server
    sqcon = mysql.connector.connect(user='admin',
                                    password='BugBase{m3s5_h1M_uP_3nT1Red}',
                                    host='database.cgsgsrn0p.us-west-1.rds.amazonaws.com',
                                    port='3306',
                                    database='data_all')
    cursor = sqcon.cursor()

    # Inserting into table
    cursor.execute(
        "update data set is_latest=0 where agency_name='spectre_corp';")
    for batch in response:
        for row in batch.results:
            cursor.execute(
                "INSERT INTO data VALUES (0, 'spectre_corp', " + str(row.campaign.id) +
                ", \"" + str(row.campaign.name) +
                "\",\"" + str(row.ad_group.campaign) +
                "\"," + str(row.metrics.cost_micros/1000000) +
                "," + str(row.metrics.impressions) +
                "," + str(row.metrics.clicks) +
                "," + str(row.metrics.cost_per_conversion/1000000) +
                "," + str(row.metrics.ctr) +
                "," + str(row.metrics.average_cpc/1000000) +
                "," + str(row.metrics.average_cpm/1000000) +
                "," + str(row.metrics.all_conversions_from_interactions_rate) +
                "," + str(row.metrics.conversions_value/1000000) +
                "," + str(row.metrics.search_impression_share) +
                "," + str(row.metrics.conversions) +
                ", 1, date(now())-1, date(now()), 1);")

    sqcon.commit()
    sqcon.close()
    print("Done gathering data")


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage(
        "/home/user/Documents/ads/google/google-ads.yaml")     # CHANGE THIS TO DIFFERENT PATH

    try:
        main(googleads_client)
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)
