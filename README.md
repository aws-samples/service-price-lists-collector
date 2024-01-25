#AWS Price Lists Collector

Welcome to this tiny project that demonstrate how to collect the price lists from AWS Pricing API and
build a single CSV document with all the information you need.


## Getting started

* `git clone git@github.com:aws-samples/service-price-lists-collector.git`
* Requires: python 3.9 to 3.11. Python3.12 is not supported by some dependencies.
* `pip install requirements.txt`
* Configure the script ***fetch_aws_pricelists.py***
* Make sure the terminal session you run this script from has credentials to an AWS Account
* `python fetch_aws_pricelists.py`

Tip: you can easily run this script from a [Cloud9](https://aws.amazon.com/pm/cloud9) environment

## What does this do?
1. Optionally store a list of all available services as a JSON document in the current directory.#. 
1. Fetch the price lists for the given regions and services (as received = raw)
1. Remove the unused columns according to the list of Used Header and store each new truncated price list in a separate directory.
1. Concatenate all the truncated price lists in a single CSV document.

## Security
See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License
This project is licensed under the MIT-0 license. See [License document](LICENSE).
