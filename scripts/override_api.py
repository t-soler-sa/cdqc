"""
Recommendation for usage
The recommended maximum number of updates per request would be 1000 members.

Maintain single list updates per request.

For concurrent requests, limit them to upto 5 requests.

Do not update the same list members across multiple concurrent requests. 
Prefer to use distinct lists across concurrent requests.

Consequent requests should be sent once the response for previous request(s) is received. 
If a timeout is received, then reduce the number of updates being made per request

"""

import requests
from datetime import datetime

# define variable modified_time with format UTC. (YYYY-MM-DDThh:mm:ss.SSSZ)
modified_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

# define origin_timestamp with format UTC date-time (YYYY-MM-DDThh:mm:ss.SSSZ)
origin_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# api-endpoint
URL = "https://santander.blackrock.com/api/reference-data/lists/v1/lists/STR_TEST_A_OK/members"

# include parameters and headers
headers = {
    "Content-Type": "application/json",
    "VND.com.blackrock.Request-ID": "65057522-ac12-11ef-8a2c-a76a025eedc8",
    "VND.com.blackrock.Origin-Timestamp": origin_timestamp,  # alternative format RFC7231 HTTP-date format (E, d M YYYY hh:mm:ss GMT)
    "VND.com.blackrock.API-Key": "TOKEN_HERE",
    "Authorization": "BASIC_AUTH_TOKEN_HERE",
}
payload = [
    {
        "comment": "test override ok",
        "listMemberType": "ApprovalListMemberEntry",  # required
        "listName": "STR_TEST_A_OK",  # Short, unique list name
        "modifiedBy": "t4tsoler",  # Login ID of the person or program that last modified this record.
        "modifiedTime": modified_time,
        "identifier": "R55746",  # required A member of an approval list, e.g, an issuer or security identifier
        "subList": "OK",  # For grouped list only, defines the specific sub-list that the member falls under. e.g. in a "Recommendations" list this would define whether the member identifier is on the "Buy", "Sell" or "Hold" sub-list.
    }
]


# The recommended maximum number of updates per request would be 1000 members

# sending post request and saving the response as response object
r = requests.post(URL, json=payload, headers=headers)

# extracting data in json format
data = r.json()

# printing the output
print(data)
