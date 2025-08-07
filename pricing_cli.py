import click
import boto3
import pandas as pd
from datetime import datetime
import requests
from botocore.exceptions import ClientError, NoCredentialsError, CredentialRetrievalError
from typing import List, Optional
import os
import sys

class CredentialsError(Exception):
    pass

class PricingDataManager:
    def __init__(self, region: str, output_dir: str = '.'):
        self.region = region
        self.output_dir = output_dir
        try:
            self.pricing_client = self._initialize_client()
        except (NoCredentialsError, CredentialRetrievalError) as e:
            raise CredentialsError(
                "AWS credentials not found. Please ensure credentials are configured:\n"
                "1. Environment variables (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)\n"
                "2. AWS credentials file (~/.aws/credentials)\n"
                "3. IAM role for EC2 instance or container\n"
                f"\nError details: {str(e)}"
            )

    def _initialize_client(self):
        sts = boto3.client('sts')
        sts.get_caller_identity()
        return boto3.client('pricing', region_name='us-east-1')

    def _check_permissions(self):
        try:
            self.pricing_client.describe_services(MaxResults=1)
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDeniedException':
                raise CredentialsError(
                    "Your credentials don't have permission to access the Pricing API. "
                    "Please ensure you have the 'pricing:DescribeServices' permission."
                )
            raise

    def get_all_service_codes(self) -> List[str]:
        service_codes = []
        paginator = self.pricing_client.get_paginator('describe_services')
        for page in paginator.paginate():
            service_codes.extend([s['ServiceCode'] for s in page['Services']])
        return service_codes
    
    def validate_service_codes(self, service_codes: List[str]) -> None:
        valid_services = set(self.get_all_service_codes())
        invalid_services = [code for code in service_codes if code not in valid_services]
        if invalid_services:
            raise ValueError(f"Invalid service code(s): {', '.join(invalid_services)}. Use 'aws pricing describe-services' to find valid codes.")
    
    def validate_region(self) -> None:
        try:
            ec2 = boto3.client('ec2', region_name='us-east-1')
            regions = ec2.describe_regions()['Regions']
            valid_regions = [r['RegionName'] for r in regions]
            if self.region not in valid_regions:
                raise ValueError(f"Invalid region '{self.region}'. Valid regions: {', '.join(sorted(valid_regions))}")
        except ClientError:
            # Fallback: try a simple pricing API call to validate region
            try:
                self.pricing_client.list_price_lists(
                    ServiceCode='AmazonEC2',
                    CurrencyCode='USD',
                    EffectiveDate=datetime.now().strftime("%Y-%m-%d %H:%M"),
                    RegionCode=self.region,
                    MaxResults=1
                )
            except ClientError as e:
                if 'region' in str(e).lower():
                    raise ValueError(f"Invalid region '{self.region}'. Please check the region name.")

    def get_price_list(self, service_code: str) -> Optional[str]:
        try:
            response = self.pricing_client.list_price_lists(
                ServiceCode=service_code,
                CurrencyCode='USD',
                EffectiveDate=datetime.now().strftime("%Y-%m-%d %H:%M"),
                RegionCode=self.region
            )
            return response['PriceLists'][0]['PriceListArn'] if response['PriceLists'] else None
        except ClientError as e:
            error_msg = e.response.get('Error', {}).get('Message', '')
            if 'region' in error_msg.lower():
                raise ValueError(f"Invalid region '{self.region}'. Please check the region name.")
            elif 'service' in error_msg.lower():
                raise ValueError(f"Invalid service code '{service_code}'. Use 'aws pricing describe-services' to find valid codes.")
            raise ValueError(f"Error fetching price list for {service_code}: {str(e)}")
    
    def process_service_codes(self, service_codes: List[str], output_format: str) -> List[str]:
        downloaded_files = []
        failed_services = []
        
        for i, code in enumerate(service_codes, 1):
            click.secho(f"\n[{i}/{len(service_codes)}] Processing {code}...", fg='cyan')
            try:
                price_list_arn = self.get_price_list(code)
                if price_list_arn:
                    file_path = self.download_pricing_data(price_list_arn, code, output_format)
                    if file_path:
                        downloaded_files.append(file_path)
                else:
                    failed_services.append(f"{code} (no pricing data available in {self.region})")
            except ValueError:
                raise
        
        if failed_services:
            click.secho("\nWarning: No pricing data found for:", fg='yellow')
            for service in failed_services:
                click.secho(f"  - {service}", fg='yellow')
        
        return downloaded_files
    
    def download_pricing_data(self, price_list_arn: str, service_code: str, file_format: str) -> Optional[str]:
        try:
            response = self.pricing_client.get_price_list_file_url(
                PriceListArn=price_list_arn, FileFormat=file_format)
            
            filename = f"{self.output_dir}/pricing-{service_code}-{self.region}.{file_format}"
            
            for attempt in range(3):
                try:
                    r = requests.get(response['Url'], stream=True, timeout=(5, 30))
                    r.raise_for_status()
                    
                    total_size = int(r.headers.get('content-length', 0))
                    
                    with open(filename, 'wb') as f:
                        if total_size > 0:
                            with click.progressbar(length=total_size, 
                                                 label=f'Downloading {service_code}',
                                                 show_percent=True) as bar:
                                for chunk in r.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                                        bar.update(len(chunk))
                        else:
                            for chunk in r.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                    
                    click.secho(f" ✓ Successfully downloaded pricing for {service_code}", fg='green')
                    return filename
                    
                except requests.Timeout:
                    if attempt == 2:
                        click.secho(f" ✗ Timeout error downloading pricing for {service_code} after 3 attempts", fg='red')
                        return None
                except requests.RequestException as e:
                    click.secho(f" ✗ Error downloading pricing for {service_code}: {str(e)}", fg='red')
                    return None
                    
        except Exception as e:
            click.secho(f" ✗ Error processing pricing for {service_code}: {str(e)}", fg='red')
            return None

    def consolidate_files(self, files: List[str], output_format: str) -> Optional[str]:
        if not files:
            return None

        consolidated_data = []
        for file in files:
            try:
                df = pd.read_csv(file, skiprows=5, low_memory=False, on_bad_lines='skip')
                service_name = os.path.basename(file).split('-')[1]
                df['ServiceCode'] = service_name
                consolidated_data.append(df)
                click.secho(f"Successfully read pricing data from {service_name}", fg='green')
            except Exception as e:
                click.secho(f"Warning: Error reading file {file}: {str(e)}", fg='yellow')
                continue

        if not consolidated_data:
            return None

        try:
            df = pd.concat(consolidated_data, ignore_index=True)
            df = df.dropna(axis=1, how='all')
            
            output_file = f"{self.output_dir}/consolidated_pricing_{self.region}.{output_format}"
            
            if output_format == 'csv':
                df.to_csv(output_file, index=False)
            elif output_format == 'json':
                df.to_json(output_file, orient='records')
            elif output_format == 'excel':
                df.to_excel(output_file, index=False)
            
            click.secho(f"\nSuccessfully consolidated {len(consolidated_data)} files", fg='green')
            return output_file
            
        except Exception as e:
            click.secho(f"Error during consolidation: {str(e)}", fg='red')
            return None
    
    def truncate_data(self, file_path: str, columns: List[str]) -> Optional[str]:
        try:
            # Check if this is a consolidated file (no metadata rows to skip)
            is_consolidated = 'consolidated_pricing' in os.path.basename(file_path)
            
            if is_consolidated:
                df = pd.read_csv(file_path, low_memory=False)
            else:
                df = pd.read_csv(file_path, skiprows=5, low_memory=False, on_bad_lines='skip')
            
            available_columns = [col for col in columns if col in df.columns]
            
            if not available_columns:
                click.secho(f"No specified columns found in the data. Available columns: {', '.join(df.columns[:10])}{'...' if len(df.columns) > 10 else ''}", fg='red')
                return None

            base_name = os.path.splitext(file_path)[0]
            extension = os.path.splitext(file_path)[1]
            output_file = f"{base_name}_truncated{extension}"
            
            df[available_columns].to_csv(output_file, index=False)
            return output_file
        except Exception as e:
            click.secho(f"Error truncating data: {str(e)}", fg='red')
            return None


def print_credential_help():
    click.secho("\nAWS Credential Configuration Guide:", fg='yellow', bold=True)
    click.secho("\n1. Using Environment Variables:", fg='yellow')
    click.echo("   Export these variables in your shell:")
    click.echo("   export AWS_ACCESS_KEY_ID='your_access_key'")
    click.echo("   export AWS_SECRET_ACCESS_KEY='your_secret_key'")
    click.echo("   export AWS_DEFAULT_REGION='your_region'")

    click.secho("\n2. Using AWS Credentials File:", fg='yellow')
    click.echo("   Create or edit ~/.aws/credentials:")
    click.echo("   [default]")
    click.echo("   aws_access_key_id = your_access_key")
    click.echo("   aws_secret_access_key = your_secret_key")

    click.secho("\n3. Using AWS CLI:", fg='yellow')
    click.echo("   Run: aws configure")

    click.secho("\n4. Using IAM Roles:", fg='yellow')
    click.echo("   If running on EC2 or ECS, attach an appropriate IAM role")
    click.echo("   with the required permissions.")

    click.secho("\nRequired Permissions:", fg='yellow')
    click.echo("Ensure your credentials have these permissions:")
    click.echo("- pricing:DescribeServices")
    click.echo("- pricing:GetProducts")
    click.echo("- pricing:ListPriceLists")
    click.echo("- pricing:GetPriceListFileUrl")

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--region', required=True, help='AWS region to fetch prices for')
@click.option('--service-code', help='Comma-separated list of AWS service codes (e.g., AmazonEC2,AmazonS3)')
@click.option('--all-services', is_flag=True, help='Fetch prices for all available services')
@click.option('--output-dir', default='.', help='Directory to save output files')
@click.option('--format', 'output_format', type=click.Choice(['csv', 'json', 'excel']), 
              default='csv', help='Output file format')
@click.option('--consolidate', is_flag=True, help='Consolidate all pricing files into one')
@click.option('--truncate', help='Comma-separated list of columns to keep in the output')
@click.option('--debug', is_flag=True, help='Enable debug mode for detailed error messages')
def fetch_pricing(region: str, service_code: str, all_services: bool, output_dir: str,
                 output_format: str, consolidate: bool, truncate: str, debug: bool):
    """AWS service pricing information fetcher with advanced features"""
    
    if not (service_code or all_services):
        click.secho("\nError: Must specify either --service-code or --all-services", 
                   fg='red', bold=True)
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
        return

    if service_code and all_services:
        click.secho("\nError: Cannot use both --service-code and --all-services", 
                   fg='red', bold=True)
        return

    try:
        os.makedirs(output_dir, exist_ok=True)
        pricing_manager = PricingDataManager(region, output_dir)
        pricing_manager._check_permissions()

        click.secho("=" * 50, fg='blue')
        click.secho(f" AWS Pricing Data Fetcher - Region: {region} ", fg='blue', bold=True)
        click.secho("=" * 50, fg='blue')

        if all_services:
            service_codes = pricing_manager.get_all_service_codes()
            if not service_codes:
                click.secho("No services found!", fg='red')
                return
        else:
            service_codes = [code.strip() for code in service_code.split(',')]
            click.secho(f"Fetching pricing for services: {', '.join(service_codes)}...", fg='yellow')
            
        # Validate region and service codes before processing
        try:
            pricing_manager.validate_region()
            if not all_services:
                pricing_manager.validate_service_codes(service_codes)
        except ValueError as e:
            click.secho(f"\nError: {str(e)}", fg='red', err=True)
            return
            
        try:
            downloaded_files = pricing_manager.process_service_codes(service_codes, output_format)
        except ValueError as e:
            click.secho(f"\nError: {str(e)}", fg='red', err=True)
            return

        if downloaded_files:
            if consolidate:
                consolidated_file = pricing_manager.consolidate_files(downloaded_files, output_format)
                if consolidated_file and truncate:
                    columns = [col.strip() for col in truncate.split(',')]
                    truncated_file = pricing_manager.truncate_data(consolidated_file, columns)
                    if truncated_file:
                        click.secho(f"Successfully truncated data to: {truncated_file}", fg='green')
            elif truncate:
                columns = [col.strip() for col in truncate.split(',')]
                for file in downloaded_files:
                    truncated_file = pricing_manager.truncate_data(file, columns)
                    if truncated_file:
                        click.secho(f"Successfully truncated data to: {truncated_file}", fg='green')
            
            click.secho(f"\nDownload completed! {len(downloaded_files)} file(s) downloaded.", fg='green', bold=True)
        else:
            click.secho("\nNo pricing data was downloaded.", fg='yellow', bold=True)

    except CredentialsError as e:
        click.secho(f"\nCredential Error: {str(e)}", fg='red', err=True)
        print_credential_help()
        sys.exit(1)
    except Exception as e:
        if debug:
            raise
        else:
            click.secho(f"\nError: {str(e)}", fg='red', err=True)
            sys.exit(1)

if __name__ == '__main__':
    fetch_pricing()