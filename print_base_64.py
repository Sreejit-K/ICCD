import base64

username = "elastic"
password = "your_password"

# Construct "username:password"
credentials = f"{username}:{password}"

# Encode as Base64
basic_auth_b64 = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

print(basic_auth_b64) # use it in the script
