# rds-slowlog-downloader
Downloads RDS(for MySQL, Aurora) slow query logs.


## Installation

Download from https://github.com/tkuchiki/rds-slowlog-downloader/releases

## Usage

```console
$ ./rds-slowlog-downloader --help
usage: rds-slowlog-downloader [<flags>]

Flags:
  --help                       Show context-sensitive help (also try --help-long and --help-man).
  --access-key=ACCESS-KEY      AWS access key ID
  --secret-key=SECRET-KEY      AWS secret access key
  --arn=ARN                    AWS Assume role ARN
  --token=TOKEN                AWS access token
  --profile=PROFILE            AWS CLI profile
  --credentials=CREDENTIALS    AWS CLI Credential file
  --region=REGION              AWS region
  --aws-config=AWS-CONFIG      AWS CLI Config file
  --instance-ids=INSTANCE-IDS  RDS instance IDs (comma separated)
  --config-file="/tmp/rds-slowlog-downloader.conf"
                               Config gile
  --output=/tmp/instance_id.slowquery.log
                               Output to slowlog
  --version                    Show application version.

```
