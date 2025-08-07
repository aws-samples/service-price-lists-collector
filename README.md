# AWS Price Lists Collector

A command-line tool that fetches AWS service pricing information and exports it to CSV format. This tool allows you to download pricing data for either specific AWS services or all available services in a given region.

## Features

- Download pricing for specific AWS services
- Bulk download pricing for all AWS services
- Export data to CSV, JSON, or Excel format
- Configurable output directory
- File consolidation with automatic empty column removal
- Column truncation for both individual and consolidated files

## Getting started

You need to be logged in to your AWS account.

1. Clone and navigate into this repository.
2. Make sure you have Python 3.6 or later installed by running: `python --version`.
3. It's recommended to create a virtual environment first: `python -m venv venv && source venv/bin/activate`
4. Install the dependencies using pip: `pip install -r requirements.txt`

Docs of the API: https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/using-the-aws-price-list-bulk-api.html

## Pricing CLI
### This tool fetches AWS service pricing information and save to CSV

#### Here are the various ways to use the pricing CLI tool:

*Get pricing for a specific service in a region:*

`python pricing_cli.py --region us-east-1 --service-code AmazonEC2`

*Get pricing for all AWS services in a region:*

`python pricing_cli.py --region us-west-2 --all-services`

*Get pricing and save to a specific directory:*

`python pricing_cli.py --region eu-central-1 --service-code AmazonS3 --output-dir /tmp/pricing`

*Get pricing for multiple services and consolidate to one file:*

`python pricing_cli.py --region us-east-1 --service-code AmazonEC2,AmazonS3 --consolidate`

*Get pricing and specify columns to keep in the output:*

`python pricing_cli.py --region us-east-1 --service-code AmazonEC2 --truncate SKU,PricePerUnit,Currency`

*Get pricing for multiple services, consolidate and truncate in one command:*

`python pricing_cli.py --region us-east-1 --service-code AmazonEC2,AmazonS3 --consolidate --truncate SKU,PricePerUnit,Currency`

*Export pricing data in different formats:*

`python pricing_cli.py --region us-east-1 --service-code AmazonEC2 --format json`

*Get help and see all available options:*

`python pricing_cli.py --help`

#### Finding AWS Service Codes

To find the correct service code for an AWS service:

1. **AWS Price List API**: Use the `describe-services` API call:
   ```bash
   aws pricing describe-services --region us-east-1 --query 'Services[].ServiceCode' --output table
   ```

2. **AWS Pricing Calculator**: Visit https://calculator.aws and browse services to see their codes

3. **AWS Documentation**: Check the [AWS Price List API documentation](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/using-the-aws-price-list-bulk-api.html) for service code references

#### Common service code examples:

```shell
#For EC2
python pricing_cli.py --region ap-southeast-1 --service-code AmazonEC2

#For RDS
python pricing_cli.py --region us-east-2 --service-code AmazonRDS

#For S3
python pricing_cli.py --region eu-west-1 --service-code AmazonS3

#For Lambda
python pricing_cli.py --region ap-northeast-1 --service-code AWSLambda
```

### Command Line Options
- `--region`:           AWS region to fetch prices for (required)
- `--service-code`:     One or more comma-separated list of AWS service codes
- `--all-services`:     Flag to fetch prices for all available services
- `--output-dir`:       Directory to save CSV files (default: current directory)
- `--format`:           Output file format [csv|json|excel]
- `--consolidate`:      Consolidate all pricing files into one
- `--truncate`:         Comma-separated list of columns to keep in the output
- `--debug`:            Enable debug mode for detailed error messages
- `-h, --help`:         Show help message and exit

### Output
- Individual files: `pricing-{service_code}-{region}.{format}`
- Consolidated files: `consolidated_pricing_{region}.{format}`
- Truncated files: `{original_filename}_truncated.{format}`
- Default output directory is the current working directory
- Supported formats: CSV, JSON, Excel


### Important Notes:

1. You must specify either `--service-code` or `--all-services`
2. The `--region` parameter is required and will be validated
3. Service codes are validated before processing begins
4. If `--output-dir` is not specified, files will be saved in the current directory
5. You need valid AWS credentials with pricing API permissions
6. Consolidation automatically removes empty columns
7. Truncation works on both individual and consolidated files


## Contributing
Contributions are welcome! Please See [CONTRIBUTING](CONTRIBUTING.md) for more information.

## Security
See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License
This project is licensed under the MIT-0 license. See [License document](LICENSE).
