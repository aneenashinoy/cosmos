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

Before running the program run pip install -r requirements.txt