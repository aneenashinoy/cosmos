For safety purposes save the aws credentials in your home folder ~/.aws/credentials and ~/.aws/config
Home folder for Windows users C:/Users/{UserName}, for Mac users use the above command
Set up the AWS home folder using aws configure


Follow the following pattern
# In ~/.aws/credentials:
[development]
aws_access_key_id=foo
aws_access_key_id=bar

# In ~/.aws/config
[profile crossaccount]
role_arn=arn:aws:iam:...
source_profile=development

For more information follow https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

# Code Setup

Before running the program run **pip install -r requirements.txt**

# Command Parameters
py dbConfig.py {env} {params}

{env}  
* dev for DEV environment
* test for SIT environment
* uat for UAT environment

{params} -
* r/retailer - create/update retailer details
* s/store - create/update store details (ecom as well)
* wms/warehouse - create/updates warehouse details
* psp/paymemt - updates the PSP details
* p/product - create/updates product details
* sfcc-inv/SFCC Inventory - create/updates the SFCC inventory details
* farfetch-inv/Farfetch Inventory - create/updates the Farfetch inventory details
* redant-inv/RedAnt Inventory - create/updates the Redant inventory details
* siocs-inv/SIOCS inventory - create/updates the SIOCS inventory details
* ceConfig - creates/updates CE config and geo code details
* ceOrder - creates/updates CE order details
* ceReturnOrder - creates/updates CE return order details
* ceProduct - creates/updates CE product details
* cePrice - creates/updates CE price details
* ceInventory - creates/updates CE inventory details

Note:- Install openpyxl and aws-shell using pip install if any issues
