# PolitiFact scraping dataset

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19062297.svg)](https://doi.org/10.5281/zenodo.19062297)

This repository contains the code to download all the content from [PolitiFact](https://www.politifact.com/) website.

To store in a MongoDB, the following environment variables should be available:

- `MONGODB_HOST` with the connection string to the cluster
- `MONGODB_USER` the user name with access permissions to the cluster
- `MONGODB_PASSWORD` the password for the user

In such case, the data will be stored in a database with name `politifact` within the cluster.
