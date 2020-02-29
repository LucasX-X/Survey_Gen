from datetime import datetime
from datetime import timedelta
import cx_Oracle
from pprint import pprint
from qualtrics_account import QualtricsAccount
from qualtrics_mailing_list import QualtricsMailingList
from qualtrics_distribution import QualtricsDistribution

# set data center and API token
api_token = 'YTptUeoO9qjzuSdKw0SLMovSFQ8FjBO9cSVLKdnc'
data_center = 'riceuniversity.co1'

# initialize Qualtrics Account object
account = QualtricsAccount(api_token, data_center)

# set library id, mailing list, and category name
now = datetime.now() - timedelta(days=1)
now = now.strftime('%d-%b-%y')
now = now.upper()
# now = '25-JUL-19'
print(now)

mailing_list_name = now
library_id = 'GR_3lzcKCuD2bzBWK1'
category_name = 'Survey'

# initialize Qualtrics Mailing List object
mailing_list = QualtricsMailingList(
    account,
    library_id,
    mailing_list_name,
    category_name
)

# import contact list from database
mailing_list.import_contact_list_from_database(now)

pprint(mailing_list.contact_list)

# set message id, survey id, and email settings
message_id = 'MS_cBWLwylcQEStwbP'
survey_id = 'SV_552uziGIGpZYQYJ'
now = datetime.now()
send_date = now.strftime('%Y-%m-%dT%H:%M:%Sz')
expiration_date = (now + timedelta(days=14, hours=5)).strftime('%Y-%m-%dT%H:%M:%Sz')
send_thankyou_date = (now + timedelta(hours=6, minutes=5)).strftime('%Y-%m-%dT%H:%M:%Sz')
from_name = 'Bart Salmon'
reply_email = 'owlsurveys@rice.edu'
subject = 'Work Order Completion Survey'
distribution = QualtricsDistribution(
    mailing_list,
    message_id,
    survey_id,
    send_date,
    expiration_date,
    from_name,
    reply_email,
    subject,
    send_thankyou_date,
)


# print survey-distribution details to confirm proper creation
# pprint(distribution.details)
