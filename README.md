#AWS Price Lists Collector

Welcome to this tiny project that demonstrate how to collect the price lists from AWS Pricing API and
build a single CSV document with all the information you need.


## Getting started

* `git clone git@ssh.gitlab.aws.dev:lautip/aws-price-lists-collector.git`
* Requires: python 3.9 to 3.11. Python3.12 is not supported by some dependencies.
* `pip install requirements.txt`
* Configure the script ***fetch_aws_pricelists.py***
* Make sure the terminal session you run this script from has credentials to an AWS Account
* `python fetch_aws_pricelists.py`

Tip: you can easily run this script from a [Cloud9](https://aws.amazon.com/pm/cloud9) environment

## License
This project is licensed under the Apache-2.0 License.
