# Configuration

This directory contains configuration templates for the geospatial data downloader.

## Quick Start

1. Copy the template:
   ```bash
   cp configs/config.template.yml config.yml
   ```

2. Set required environment variables, or save them into a `.env` file:
   ```bash
   ODATA_USERNAME="your_copernicus_username"
   ODATA_PASSWORD="your_copernicus_password"
   EARTHDATA_USERNAME="your_nasa_username"
   EARTHDATA_PASSWORD="your_nasa_password"
   EUMETSAT_CONSUMER_KEY="your_eumetsat_key"
   EUMETSAT_CONSUMER_SECRET="your_eumetsat_secret"
   ```

3. Run the CLI :
   ```bash
  eokit [options]
   ```

## Configuration Sections

### `download`
Settings for each downloader required, to tune performance and reliability.

### `auth`
Authentication credentials for different data providers. All sensitive values use environment variables.

### `sources`
Data source definitions. Each source specifies:
- `authenticator` - Which auth provider to use
- `downloader` - Download method
- `composite` - Which band composition to load
- `search_limit` - Max results per search
- Provider-specific settings (STAC URLs, collections, etc.)

### `writer`
Output format settings, currently supports GeoTIFF with compression options.
