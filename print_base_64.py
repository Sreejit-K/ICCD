import base64

username = "elastic"
# password = "8fwbD6HbJh6HU0oddsHm8TEI"  demo

password = "ZDRlODI0MTA3MWZiMTFlZmFk"  

# Construct "username:password"
credentials = f"{username}:{password}"

# Encode as Base64
basic_auth_b64 = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

print(basic_auth_b64) # use it in the script


#  demo - ZWxhc3RpYzo4ZndiRDZIYkpoNkhVMG9kZHNIbThURUk=

#  dev  - ZWxhc3RpYzpaRFJsT0RJME1UQTNNV1ppTVRGbFptRms=