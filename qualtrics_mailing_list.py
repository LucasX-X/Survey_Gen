"""Creates a mailing-list class for interfacing with Qualtrics API v3
This module creates a class for encapsulating a Qualtrics Mailing List based
upon a Qualtrics Account object, Qualtrics Library/Group id, and settings for
mailing-list name and category name
"""

import json
import csv
import sys
import pandas as pd
import requests
import cx_Oracle
from qualtrics_account import QualtricsAccount


class QualtricsMailingList(object):
    """Mailing-list Class for interfacing with Qualtrics API v3"""
    def __init__(
            self,
            account: QualtricsAccount,
            library_id: str,
            mailing_list_name: str,
            category_name: str
    ):
        """Initializes a Qualtrics mailing-list object
        Args:
            account: a QualtricsAccount object
            library_id: a Qualtrics Library/Group id, which acts similarly to a
                "working directory"; see https://api.qualtrics.com/docs/
                finding-qualtrics-ids
            mailing_list_name: the name for the mailing list being created
            category_name: the category name for the mailing list being created,
                which acts similarly to a "tag" for the mailing list
        """
        self.account = account
        self.library_id = library_id
        self.mailing_list_name = mailing_list_name
        self.category_name = category_name

        # make Qualtrics API v3 call to create mailing list
        request_response = requests.request(
            "POST",
            f"https://{self.account.data_center}.qualtrics.com"
            f"/API/v3/mailinglists/",
            data=(
                f'{{"libraryId": "{library_id}",'
                f'"name": "{mailing_list_name}",'
                f'"category": "{category_name}" }}'
            ),
            headers={
                "content-type": "application/json",
                "x-api-token": self.account.api_token,
            },
        )

        # extract mailing list id from HTTP response
        self.id = request_response.json()["result"]["id"]

    def import_contact_list_from_database(self, date) -> None:

        # connect the Oracle database
        dsn_tns = cx_Oracle.makedsn('wyoming.rice.edu', '1521', service_name='dwdprod.rice.edu')
        con = cx_Oracle.connect(user='WHFEPMGR', password='FEP4N4LYTICS', dsn=dsn_tns)
        # con = cx_Oracle.connect(user='FEP_Python', password='Pyth0nF0rF3P', dsn=dsn_tns)

        cur = con.cursor()

        queryString1 = "SELECT REQOR_FIRST_NAME, REQOR_LAST_NAME, REQOR_EMAIL, REQ_STATEMENT_OF_WORK, REQ_ID "
        queryString2 = "FROM V_FEP_WORK_ORDER_DETAILS WHERE REQ__WORK_COMPLETE_DT LIKE '{date}%' AND REQ_STATUS LIKE 'Complete' ".format(date=date)
        queryString3 = "AND REQ_TYPE in ('Events', 'Elevators &' || ' Lifts', 'Carpentry', 'Electrical', 'Exterior Building', " \
                       "'HVACR', 'Interior Building', 'Leaks', 'Mechanical', 'Painting', 'Plumbing', 'Vehicle &' || ' Equipment Repair', " \
                       "'Grounds/Landscaping', 'Moving', 'Odor', 'Pest Control', 'Pest/Animal Control', 'Pest/AnimalControl', 'Custodial') "
        queryString4 = "AND CREWNAME IN ('01 - Air Conditioning', '02 - Plumbing-Ext-Mech Repair', '03 - Electrical'," \
                       "'04 - Carpentry - Painting', '07 - Preventive Maintenance', '08 - Elevators','11 - Grounds', '12 - Movers-Solid Waste', " \
                       "'13 - Custodial South', '13 - Custodial East', '13 - Custodial North', '13 - Custodial Admin', " \
                       "'13 - Custodial West','17 - Equipment Repair', '24 - Arboriculture')"
        queryString5 = "AND CREWNAME IN ('08 - Elevators','11 - Grounds', '12 - Movers-Solid Waste', " \
                       "'13 - Custodial South', '13 - Custodial East', '13 - Custodial North', '13 - Custodial Admin', " \
                       "'13 - Custodial West','17 - Equipment Repair', '24 - Arboriculture')"

        # if int(date[0:2]) % 2 == 1:
        #     queryString = queryString1 + queryString2 + queryString3 + queryString4
        # else:
        #     queryString = queryString1 + queryString2 + queryString3 + queryString5

        queryString = queryString1 + queryString2 + queryString3 + queryString4

        cur.execute(queryString)

        # fetch the data and description
        data = cur.fetchall()
        fields = cur.description

        cur.close()
        con.close()

        # exit if no data
        if not data:
            sys.exit()

        contact_list = []
        # transfer it from table to json and elimate the duplicate
        duplicate_checker = []
        for row in data:
            record = {}
            for i in range(len(fields)):
                record[fields[i][0]] = row[i]
            record['DATE'] = date
            if record['REQOR_EMAIL'] not in duplicate_checker:
                duplicate_checker.append(record['REQOR_EMAIL'])
                contact_list.append(record)

        contact_list_log = contact_list

        # output log file
        with open('C:/Users/tlm6/Desktop/{}.CSV'.format(date), 'w') as csvfile:
            fieldnames = ['REQOR_FIRST_NAME', 'REQOR_LAST_NAME', 'REQOR_EMAIL', 'REQ_STATEMENT_OF_WORK', 'REQ_ID', 'DATE']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(contact_list_log)
            csvfile.close()

        # change the content make it legal to be sent
        t = []
        for i in range(len(contact_list)):
            d = {}
            d['firstName'] = contact_list[i]['REQOR_FIRST_NAME']
            d['lastName'] = contact_list[i]['REQOR_LAST_NAME']
            d['email'] = contact_list[i]['REQOR_EMAIL']
            del contact_list[i]['REQOR_FIRST_NAME']
            del contact_list[i]['REQOR_LAST_NAME']
            del contact_list[i]['REQOR_EMAIL']
            d['embeddedData'] = contact_list[i]
            t.append(d)


        contact_list = t[:]

        # make Qualtrics API v3 call to upload contact list
        request_response = requests.request(
            "POST",
            f"https://{self.account.data_center}.qualtrics.com"
            f"/API/v3/mailinglists/{self.id}/contactimports",
            json={"contacts": contact_list},
            headers={
                "content-type": "application/json",
                "x-api-token": self.account.api_token,
            },
        )

        # check upload progress until complete
        progress_id = request_response.json()["result"]["id"]
        request_check_progress = 0
        while request_check_progress < 100:
            request_response = requests.request(
                "GET",
                f"https://{self.account.data_center}.qualtrics.com"
                f"/API/v3/mailinglists/{self.id}/contactimports/{progress_id}",
                headers={
                    "x-api-token": self.account.api_token,
                },
            )
            request_check_progress = request_response.json()["result"][
                "percentComplete"]



    @property
    def contact_list(self) -> dict:
        """Returns mailing list's contact list without caching"""
        request_response = requests.request(
            "GET",
            f"https://{self.account.data_center}.qualtrics.com"
            f"/API/v3/mailinglists/{self.id}/contacts",
            headers={
                "x-api-token": self.account.api_token,
            },
        )
        return request_response.json()["result"]["elements"]