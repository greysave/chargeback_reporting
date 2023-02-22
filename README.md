# Telus

This is the telus custom reporint app to provide chargeback to customers based upon tenancy.

######################################

# Components

The custome reporting applicaiton consists of the follwoing components:

- Docker file to build the reporiing app image based upon Alpine Linux
- custom_reporting.py main application file
- cohesity_api_helper package to help with the Cohesity APIs

######################################

- Build the container image
- Start the Container
- Execute Container command for custom_reporting.py

#######################################

# Current Issues

- Files save to the local location.  I need to work with the Telus team to have a target and API credentials to write to that target.
- Need an environment to test scale
- Only summarized data, granular data is not currently provide
