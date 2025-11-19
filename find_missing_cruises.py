#!/usr/bin/env python3
"""Find missing cruises and insert them directly into the database"""

import os
import requests
import time
import psycopg2
from concurrent.futures import ThreadPoolExecutor, wait
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

load_dotenv()

MAX_RETRIES = 3
API_TIMEOUT = 10

class DatabaseConnection:
    """Context manager for persistent database connection"""
    def __init__(self):
        self.tunnel = None
        self.conn = None

    def __enter__(self):
        ssh_host = os.getenv('SSH_HOST')
        ssh_port = int(os.getenv('SSH_PORT', '22'))
        ssh_user = os.getenv('SSH_USER')
        ssh_password = os.getenv('SSH_PASSWORD')
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = int(os.getenv('DB_PORT', '5435'))
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')

        print("Connecting to SSH tunnel...", flush=True)
        self.tunnel = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_password=ssh_password,
            remote_bind_address=(db_host, db_port),
            allow_agent=False,
            host_pkey_directories=[]
        )
        self.tunnel.start()
        print("SSH tunnel established", flush=True)

        print("Connecting to database...", flush=True)
        self.conn = psycopg2.connect(
            host='localhost',
            port=self.tunnel.local_bind_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        print("Database connected", flush=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
        if self.tunnel:
            try:
                self.tunnel.close()
                self.tunnel.stop()
                print("SSH tunnel closed", flush=True)
            except Exception:
                pass

    def query(self, sql):
        """Execute a query and return results"""
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)

            if sql.strip().upper().startswith('SELECT'):
                rows = cursor.fetchall()
                if not rows:
                    return []
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
            else:
                self.conn.commit()
                return True
        except Exception as e:
            print(f"Database error: {e}", flush=True)
            if self.conn:
                self.conn.rollback()
            if sql.strip().upper().startswith('SELECT'):
                return []
            raise
        finally:
            if cursor:
                cursor.close()

VESSELS = {
    "Roger Revelle": "R/V ROGER REVELLE",
    "Robert Gordon Sproul": "R/V ROBERT GORDON SPROUL",
    "Sally Ride": "R/V SALLY RIDE"
}

def get_r2r_cruises(vessel_short):
    """Get all cruise data including PI and port info"""
    url = f"https://service.rvdata.us/api/cruise?vessel={vessel_short}"
    print(f"Fetching R2R data for {vessel_short}...", flush=True)

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=API_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if 'data' in data:
                    cruises = {}
                    for c in data['data']:
                        if c.get('vessel_name') == vessel_short:
                            cruises[c.get('cruise_id', '')] = {
                                'cruise_name': c.get('cruise_name', ''),
                                'depart_date': c.get('depart_date', ''),
                                'arrive_date': c.get('arrive_date', ''),
                                'chief_scientist': c.get('chief_scientist', ''),
                                'depart_port': c.get('depart_port_fullname', ''),
                                'arrive_port': c.get('arrive_port_fullname', ''),
                                'operator_name': c.get('operator_name', ''),
                                'operator_id': c.get('operator_id', ''),
                                'latitude_min': c.get('latitude_min', ''),
                                'latitude_max': c.get('latitude_max', ''),
                                'longitude_min': c.get('longitude_min', ''),
                                'longitude_max': c.get('longitude_max', ''),
                            }
                    print(f"R2R data for {vessel_short} retrieved ({len(cruises)} cruises)", flush=True)
                    return cruises
                return {}  # Success but no data
        except requests.exceptions.Timeout:
            if attempt < 2:
                wait_time = 2 ** attempt
                print(f"R2R API timeout for {vessel_short} (attempt {attempt + 1}/3), retrying in {wait_time}s...", flush=True)
                time.sleep(wait_time)
            else:
                print(f"R2R API timeout for {vessel_short} after 3 attempts - skipping this vessel", flush=True)
                return {}
        except Exception as e:
            if attempt < 2:
                wait_time = 2 ** attempt
                print(f"Error fetching R2R data for {vessel_short} (attempt {attempt + 1}/3): {e}, retrying in {wait_time}s...", flush=True)
                time.sleep(wait_time)
            else:
                print(f"Error fetching R2R cruises for {vessel_short} after 3 attempts: {e}", flush=True)
                return {}

def parse_pi_name(full_name):
    """Parse 'Last, First' into components"""
    if not full_name or ',' not in full_name:
        return full_name, '', full_name
    parts = full_name.split(',', 1)
    return full_name, parts[1].strip(), parts[0].strip()

def get_port_vocabulary():
    """Fetch port vocabulary from R2R API"""
    url = "https://service.rvdata.us/api/vocabulary/?type=port"
    print("Fetching R2R port vocabulary...", flush=True)

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=API_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if 'data' in data:
                    # Create lookup by port name
                    port_lookup = {}
                    for port in data['data']:
                        name = port.get('name', '')
                        if name:
                            port_lookup[name] = {
                                'latitude': port.get('latitude', ''),
                                'longitude': port.get('longitude', ''),
                                'r2r_id': port.get('id', ''),
                                'country_id3': port.get('country_id3', '')
                            }
                    print(f"Port vocabulary retrieved ({len(port_lookup)} ports)", flush=True)
                    return port_lookup
            return {}
        except Exception as e:
            if attempt < 2:
                wait_time = 2 ** attempt
                print(f"R2R API timeout (attempt {attempt + 1}/3), retrying in {wait_time}s...", flush=True)
                time.sleep(wait_time)
            else:
                print(f"Error fetching port vocabulary: {e}", flush=True)
                return {}

def main():
    start_time = time.time()

    # ===== PHASE 1: Fetch ALL R2R data in PARALLEL (NO database, just HTTP) =====
    print("Phase 1: Fetching R2R data in parallel...", flush=True)

    all_r2r_cruises = {}
    port_vocab = {}

    # Fetch R2R cruise data for all vessels in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        vessel_futures = {executor.submit(get_r2r_cruises, short): full for short, full in VESSELS.items()}
        port_vocab_future = executor.submit(get_port_vocabulary)

        # Wait for ALL futures to complete
        try:
            wait(list(vessel_futures.keys()) + [port_vocab_future])
        except KeyboardInterrupt:
            pass  # Ignore spurious interrupt - futures complete successfully anyway

        # Collect results (all futures are done, no blocking/waiting)
        port_vocab = port_vocab_future.result()
        for future, full in vessel_futures.items():
            for cruise_id, cruise_data in future.result().items():
                all_r2r_cruises[cruise_id] = {**cruise_data, 'vessel': full}

    print(f"✓ Fetched {len(all_r2r_cruises)} cruises from R2R", flush=True)

    if not all_r2r_cruises:
        print("No cruises fetched from R2R - exiting", flush=True)
        return

    # ===== PHASE 2: ALL database operations (with persistent connection) =====
    print("\nPhase 2: Database operations...", flush=True)

    with DatabaseConnection() as db:
        # Get ALL GDC cruises for all 3 vessels in ONE query
        vessel_list = "', '".join(VESSELS.values())
        all_gdc_result = db.query(f"SELECT cruise_id, vessel FROM cruise_info WHERE vessel IN ('{vessel_list}')")
        gdc_by_vessel = {v: set() for v in VESSELS.values()}
        for row in all_gdc_result:
            gdc_by_vessel[row['vessel']].add(row['cruise_id'])

        # Find missing cruises and collect PIs/Ports
        missing_cruises_map = {}
        all_pis = {}
        all_ports = set()

        for cruise_id, cruise_data in all_r2r_cruises.items():
            vessel = cruise_data['vessel']

            # Check if cruise exists in GDC
            if cruise_id not in gdc_by_vessel[vessel]:
                missing_cruises_map[cruise_id] = cruise_data

                # Collect PI info
                if cruise_data['chief_scientist']:
                    full_name, first, last = parse_pi_name(cruise_data['chief_scientist'])
                    if full_name not in all_pis:
                        all_pis[full_name] = {
                            'first': first,
                            'last': last,
                            'institution': cruise_data['operator_name'],
                            'r2r_institution': cruise_data['operator_id']
                        }

                # Collect Port info
                if cruise_data['depart_port']:
                    all_ports.add(cruise_data['depart_port'])
                if cruise_data['arrive_port']:
                    all_ports.add(cruise_data['arrive_port'])

        print(f"Checking for missing PIs and Ports...", flush=True)

        # Check existing PIs
        existing_pis = set()
        if all_pis:
            pi_list = "', '".join([name.replace("'", "''") for name in all_pis.keys()])
            pi_results = db.query(f"SELECT pi_name FROM pi_info WHERE pi_name IN ('{pi_list}')")
            existing_pis = {row['pi_name'] for row in pi_results}

        # Check existing Ports
        existing_ports = set()
        if all_ports:
            port_list = "', '".join([name.replace("'", "''") for name in all_ports])
            port_results = db.query(f"SELECT port_name FROM port_info WHERE port_name IN ('{port_list}')")
            existing_ports = {row['port_name'] for row in port_results}

        missing_pis = {k: v for k, v in all_pis.items() if k not in existing_pis}
        missing_ports = all_ports - existing_ports

        print(f"Found: {len(missing_cruises_map)} missing cruises, {len(missing_pis)} missing PIs, {len(missing_ports)} missing ports", flush=True)

        if not missing_cruises_map:
            print("No missing cruises to insert", flush=True)
            return

        # ===== INSERT PIs =====
        if missing_pis:
            pi_values = []
            for pi_name, pi_data in missing_pis.items():
                first = (pi_data.get('first') or '').replace("'", "''")
                last = (pi_data.get('last') or '').replace("'", "''")
                inst = (pi_data.get('institution') or '').replace("'", "''")
                r2r_inst = (pi_data.get('r2r_institution') or '').replace("'", "''")
                full = pi_name.replace("'", "''")
                pi_values.append(f"('{full}', '{first}', '{last}', '{inst}', '{r2r_inst}')")

            pi_sql = "INSERT INTO pi_info (pi_name, pi_first_name, pi_last_name, pi_institution, pi_r2r_institution) VALUES\n"
            pi_sql += ',\n'.join(pi_values) + ';'
            db.query(pi_sql)
            print(f"✓ Inserted {len(missing_pis)} PIs", flush=True)

        # ===== INSERT Ports =====
        if missing_ports:
            port_values = []
            for port_name in missing_ports:
                vocab_data = port_vocab.get(port_name, {})
                safe_name = port_name.replace("'", "''")
                lat = vocab_data.get('latitude') or 'NULL'
                lon = vocab_data.get('longitude') or 'NULL'
                r2r_id = vocab_data.get('r2r_id', '')
                country = (vocab_data.get('country_id3') or port_name.split(', ')[-1]).replace("'", "''")
                port_values.append(f"('{safe_name}', '{r2r_id}', {lat}, {lon}, '{country}')")

            port_sql = "INSERT INTO port_info (port_name, r2r_id, latitude, longitude, port_country) VALUES\n"
            port_sql += ',\n'.join(port_values) + ';'
            db.query(port_sql)
            print(f"✓ Inserted {len(missing_ports)} Ports", flush=True)

        # ===== GET FOREIGN KEY IDs =====
        pi_id_map = {}
        port_id_map = {}

        # Get PI IDs
        if missing_pis:
            pi_names = "', '".join([name.replace("'", "''") for name in missing_pis.keys()])
            pi_results = db.query(f"SELECT id, pi_name FROM pi_info WHERE pi_name IN ('{pi_names}')")
            for row in pi_results:
                pi_id_map[row['pi_name']] = row['id']

        # Get Port IDs
        if missing_ports:
            port_names = "', '".join([name.replace("'", "''") for name in missing_ports])
            port_results = db.query(f"SELECT id, port_name FROM port_info WHERE port_name IN ('{port_names}')")
            for row in port_results:
                port_id_map[row['port_name']] = row['id']

        # ===== INSERT Cruises =====
        cruise_values = []
        for cruise_id, cruise_data in missing_cruises_map.items():
            safe_id = cruise_id.replace("'", "''")
            title = (cruise_data.get('cruise_name') or '').replace("'", "''")
            vessel = (cruise_data.get('vessel') or '').replace("'", "''")
            start = cruise_data.get('depart_date', 'NULL')
            end = cruise_data.get('arrive_date', 'NULL')

            # Get foreign key IDs
            pi_name = cruise_data.get('chief_scientist') or ''
            port_start = cruise_data.get('depart_port') or ''
            port_end = cruise_data.get('arrive_port') or ''

            pi_id = pi_id_map.get(pi_name, 'NULL') if pi_name else 'NULL'
            port_start_id = port_id_map.get(port_start, 'NULL') if port_start else 'NULL'
            port_end_id = port_id_map.get(port_end, 'NULL') if port_end else 'NULL'

            # Normalize NULL values
            lat_min = cruise_data.get('latitude_min') or 'NULL'
            lat_max = cruise_data.get('latitude_max') or 'NULL'
            lon_min = cruise_data.get('longitude_min') or 'NULL'
            lon_max = cruise_data.get('longitude_max') or 'NULL'
            lat_min = 'NULL' if lat_min in ('None', 'NULL') else lat_min
            lat_max = 'NULL' if lat_max in ('None', 'NULL') else lat_max
            lon_min = 'NULL' if lon_min in ('None', 'NULL') else lon_min
            lon_max = 'NULL' if lon_max in ('None', 'NULL') else lon_max

            inst = cruise_data.get('operator_name', '').replace("'", "''")

            # Format dates
            start_str = f"'{start}'" if start and start != 'NULL' else 'NULL'
            end_str = f"'{end}'" if end and end != 'NULL' else 'NULL'

            cruise_values.append(f"('{safe_id}', '{title}', '{vessel}', {start_str}, {end_str}, {pi_id}, {port_start_id}, {port_end_id}, {lat_min}, {lat_max}, {lon_min}, {lon_max}, '{inst}')")

        cruise_sql = "INSERT INTO cruise_info (cruise_id, cruise_title, vessel, cruise_start_date, cruise_end_date, pi_db_id, port_start_db_id, port_end_db_id, latitude_minimum, latitude_maximum, longitude_minimum, longitude_maximum, institution) VALUES\n"
        cruise_sql += ',\n'.join(cruise_values) + ';'
        db.query(cruise_sql)

        print(f"\n✓ Inserted: {len(missing_pis)} PIs, {len(missing_ports)} Ports, {len(missing_cruises_map)} Cruises ({time.time() - start_time:.2f}s)\n")

if __name__ == "__main__":
    main()
