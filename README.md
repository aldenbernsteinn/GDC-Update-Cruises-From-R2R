# GDC Database Synchronization Tool

This script updates the GDC database for three SIO vessels: Roger Revelle, Sally Ride, and Sproul

## Setup

### 1. Install uv

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
# Or on macOS/Linux with Homebrew:
brew install uv
```

### 2. Install Dependencies

```bash
# Install project dependencies from pyproject.toml
uv sync
```

### 3. Configure Environment

In the project root, create a `.env` file and paste this in, filling in "BLANK" with your credentials

```bash
# SSH Tunnel Config
SSH_HOST=gdc.ucsd.edu
SSH_PORT=22
SSH_USER=BLANK
SSH_PASSWORD=BLANK

# Database Config
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cruise
DB_USER=BLANK
DB_PASSWORD=BLANK
```

## 4. Grant Permissions: Ask your admin to run these queries and replace "usernamehere" with your username.

  GRANT INSERT ON pi_info, port_info, cruise_info TO usernamehere;
  GRANT USAGE, SELECT ON SEQUENCE pi_info_id_seq TO usernamehere;
  GRANT USAGE, SELECT ON SEQUENCE port_info_id_seq TO usernamehere;
  GRANT USAGE, SELECT ON SEQUENCE cruise_info_id_seq TO usernamehere;

## 5. Use this comamnd to run the script:

`uv run python find_missing_cruises.py`

## What the Script Does

`find_missing_cruises.py`:
1. Fetches all cruises from R2R API (3 vessels in parallel)
2. Compares with existing GDC cruises to find missing ones
3. Inserts missing PIs and Ports (with coordinates from R2R vocabulary)
4. Inserts missing cruises with foreign key references

## Data Sources & Field Mapping

### R2R Cruise API
**Endpoint**: `https://service.rvdata.us/api/cruise?vessel={vessel}`

Provides per cruise:
- **Cruise info**: `cruise_id`, `cruise_name`, `vessel_name`, `depart_date`, `arrive_date`
- **Cruise bounding box**: `latitude_min`, `latitude_max`, `longitude_min`, `longitude_max` (geographic extent of cruise)
- **PI info**: `chief_scientist` (parsed to first/last name), `operator_name`, `operator_id`
- **Port names**: `depart_port_fullname`, `arrive_port_fullname` (used to lookup port details)

### R2R Port Vocabulary API
**Endpoint**: `https://service.rvdata.us/api/vocabulary/?type=port`

Provides per port (looked up by port name from cruise API):
- **Port coordinates**: `latitude`, `longitude` (port location, not cruise location)
- **Port IDs**: `id` (r2r_id), `wpi_code` (World Port Index)
- **Country**: `country_id3`

Tables:
- `cruise_info` - Has foreign keys to ↓
- `pi_info` - PI records
- `port_info` - Port records with coordinates

