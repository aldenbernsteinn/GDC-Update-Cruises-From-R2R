#!/usr/bin/env python3
"""Find missing cruises and insert them directly into the database"""

import os
import re
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
    """Parse PI name, return (original, normalized, first_only, last).

    Fixes: "Smith, Craig R." matches "Smith, Craig" to prevent duplicates.
    """
    if not full_name or ',' not in full_name:
        return full_name, full_name, '', full_name

    parts = full_name.split(',', 1)
    last_name = parts[0].strip()
    first_part = parts[1].strip()
    words = first_part.split()

    if not words:
        return full_name, last_name, '', last_name

    if len(words) >= 2 and len(words[0].rstrip('.')) == 1:
        first_only = words[1]
        normalized = f"{last_name}, {words[1]}"
    elif len(words) == 1:
        first_only = words[0]
        normalized = f"{last_name}, {words[0]}"
    else:
        first_only = words[0]
        second = words[1]
        if len(second.rstrip('.')) <= 2:
            normalized = f"{last_name}, {words[0]}"
        else:
            normalized = f"{last_name}, {words[0]} {words[1]}"

    return full_name, normalized, first_only, last_name

def normalize_institution(name):
    """Normalize institution for matching. Fixes: "Texas A&M" matches "Texas A and M"."""
    if not name:
        return ''
    n = name.lower()
    n = n.replace('&', ' and ')
    n = re.sub(r'[,.-]', ' ', n)
    n = ' '.join(n.split())
    return n

def sql_escape(val):
    """Escape single quotes for SQL."""
    return (val or '').replace("'", "''")

def sql_val(v):
    """Return SQL-safe value or NULL."""
    return 'NULL' if not v or v in ('None', 'NULL', '') else v

def sql_list(items):
    """Format list for SQL IN clause."""
    return "', '".join([sql_escape(x) for x in items])

def normalize_port_name(port_name, port_vocab):
    """Append country to port name. Fixes: R2R "San Diego, California" -> DB "San Diego, California, USA"."""
    if not port_name:
        return port_name
    vocab_data = port_vocab.get(port_name, {})
    country = vocab_data.get('country_id3', '')
    if country:
        return f"{port_name}, {country.upper()}"
    return port_name

def get_r2r_vocabulary(vocab_type, key_field, field_map):
    """Fetch vocabulary from R2R API. key_field is lookup key, field_map maps output->source fields."""
    url = f"https://service.rvdata.us/api/vocabulary/?type={vocab_type}"
    print(f"Fetching R2R {vocab_type} vocabulary...", flush=True)

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=API_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if 'data' in data:
                    lookup = {}
                    for item in data['data']:
                        key = item.get(key_field, '')
                        if key:
                            lookup[key] = {out: item.get(src, '') for out, src in field_map.items()}
                    print(f"{vocab_type.title()} vocabulary retrieved ({len(lookup)} items)", flush=True)
                    return lookup
            return {}
        except Exception as e:
            if attempt < 2:
                wait_time = 2 ** attempt
                print(f"R2R API timeout (attempt {attempt + 1}/3), retrying in {wait_time}s...", flush=True)
                time.sleep(wait_time)
            else:
                print(f"Error fetching {vocab_type} vocabulary: {e}", flush=True)
                return {}

def main():
    start_time = time.time()

    # Fetch R2R data in parallel
    print("Phase 1: Fetching R2R data in parallel...", flush=True)

    all_r2r_cruises = {}
    port_vocab = {}
    person_vocab = {}
    org_vocab = {}

    with ThreadPoolExecutor(max_workers=6) as executor:
        vessel_futures = {executor.submit(get_r2r_cruises, short): full for short, full in VESSELS.items()}
        port_vocab_future = executor.submit(get_r2r_vocabulary, 'port', 'name', {'latitude': 'latitude', 'longitude': 'longitude', 'r2r_id': 'id', 'country_id3': 'country_id3'})
        person_vocab_future = executor.submit(get_r2r_vocabulary, 'person', 'name', {'org_shortname': 'organization', 'r2r_id': 'id'})
        org_vocab_future = executor.submit(get_r2r_vocabulary, 'organization', 'acronym', {'name': 'name', 'r2r_id': 'id'})

        try:
            wait(list(vessel_futures.keys()) + [port_vocab_future, person_vocab_future, org_vocab_future])
        except KeyboardInterrupt:
            pass

        port_vocab = port_vocab_future.result()
        person_vocab = person_vocab_future.result()
        org_vocab = org_vocab_future.result()
        for future, full in vessel_futures.items():
            for cruise_id, cruise_data in future.result().items():
                all_r2r_cruises[cruise_id] = {**cruise_data, 'vessel': full}

    print(f"✓ Fetched {len(all_r2r_cruises)} cruises from R2R", flush=True)

    if not all_r2r_cruises:
        print("No cruises fetched from R2R - exiting", flush=True)
        return

    # Database operations
    print("\nPhase 2: Database operations...", flush=True)

    with DatabaseConnection() as db:
        vessel_list = "', '".join(VESSELS.values())
        all_gdc_result = db.query(f"SELECT cruise_id, vessel FROM cruise_info WHERE vessel IN ('{vessel_list}')")
        gdc_by_vessel = {v: set() for v in VESSELS.values()}
        for row in all_gdc_result:
            gdc_by_vessel[row['vessel']].add(row['cruise_id'])

        gdc_institutions = {}
        inst_results = db.query("SELECT DISTINCT pi_institution FROM pi_info WHERE pi_institution IS NOT NULL")
        for row in inst_results:
            inst = row['pi_institution']
            if inst:
                norm = normalize_institution(inst)
                if norm not in gdc_institutions:
                    gdc_institutions[norm] = inst

        # Build country code -> full name map from GDC
        country_map = {}
        country_counts = {}
        port_results = db.query("SELECT port_name, port_country FROM port_info WHERE port_country IS NOT NULL")
        for row in port_results:
            port_name = row['port_name'] or ''
            port_country = row['port_country'] or ''
            parts = port_name.rsplit(', ', 1)
            if len(parts) == 2 and len(parts[1]) <= 4:
                code = parts[1].lower()
                if code not in country_counts:
                    country_counts[code] = {}
                country_counts[code][port_country] = country_counts[code].get(port_country, 0) + 1
        for code, names in country_counts.items():
            country_map[code] = max(names, key=names.get)

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
                    original_name, normalized, first_only, last = parse_pi_name(cruise_data['chief_scientist'])
                    if original_name not in all_pis:
                        person_data = person_vocab.get(original_name) or person_vocab.get(normalized, {})
                        org_shortname = person_data.get('org_shortname', '')
                        org_data = org_vocab.get(org_shortname, {})
                        r2r_inst = org_data.get('name', '')
                        norm_inst = normalize_institution(r2r_inst)
                        final_inst = gdc_institutions.get(norm_inst, r2r_inst)
                        all_pis[original_name] = {
                            'original': original_name,
                            'normalized': normalized,
                            'first': first_only,
                            'last': last,
                            'institution': final_inst,
                            'r2r_id': person_data.get('r2r_id', ''),
                            'r2r_institution': org_data.get('r2r_id', '')
                        }

                if cruise_data['depart_port']:
                    normalized_depart = normalize_port_name(cruise_data['depart_port'], port_vocab)
                    all_ports.add(normalized_depart)
                if cruise_data['arrive_port']:
                    normalized_arrive = normalize_port_name(cruise_data['arrive_port'], port_vocab)
                    all_ports.add(normalized_arrive)

        print(f"Checking for missing PIs and Ports...", flush=True)

        gdc_r2r_ids = set()
        gdc_name_inst = set()
        gdc_names = set()
        gdc_pi_map = {}
        gdc_pi_inst = {}  # pi_name -> institution

        if all_pis:
            all_gdc_pis = db.query("SELECT id, pi_name, pi_r2r_id, pi_institution FROM pi_info")
            for row in all_gdc_pis:
                if row['pi_r2r_id']:
                    gdc_r2r_ids.add(row['pi_r2r_id'])
                _, norm_name, _, _ = parse_pi_name(row['pi_name'])
                norm_inst = normalize_institution(row['pi_institution'] or '')
                gdc_name_inst.add((norm_name, norm_inst))
                gdc_names.add(row['pi_name'])
                gdc_pi_map[row['pi_name']] = row['id']
                gdc_pi_map[norm_name] = row['id']
                gdc_pi_inst[row['pi_name']] = row['pi_institution'] or ''

        # Check if PI exists by r2r_id or (name, institution) tuple
        existing_pis = set()
        for pi_name, pi_data in all_pis.items():
            r2r_id = pi_data.get('r2r_id')
            norm_name = pi_data.get('normalized', '')
            norm_inst = normalize_institution(pi_data.get('institution', ''))

            if r2r_id and r2r_id in gdc_r2r_ids:
                existing_pis.add(pi_name)
            elif (norm_name, norm_inst) in gdc_name_inst:
                existing_pis.add(pi_name)

        # Check existing Ports
        existing_ports = set()
        if all_ports:
            port_results = db.query(f"SELECT port_name FROM port_info WHERE port_name IN ('{sql_list(all_ports)}')")
            existing_ports = {row['port_name'] for row in port_results}

        missing_pis = {k: v for k, v in all_pis.items() if k not in existing_pis}
        missing_ports = all_ports - existing_ports

        print(f"Found: {len(missing_cruises_map)} missing cruises, {len(missing_pis)} missing PIs, {len(missing_ports)} missing ports", flush=True)

        if not missing_cruises_map:
            print("No missing cruises to insert", flush=True)
            return

        # ===== INSERT PIs =====
        pi_name_remap = {}
        first_conflict = True
        if missing_pis:
            pi_values = []
            for pi_name, pi_data in missing_pis.items():
                first = sql_escape(pi_data.get('first'))
                last = sql_escape(pi_data.get('last'))
                inst = pi_data.get('institution', '')
                r2r_id = sql_val(pi_data.get('r2r_id'))
                r2r_inst = sql_escape(pi_data.get('r2r_institution'))

                insert_name = pi_name
                if pi_name in gdc_names:
                    existing_inst = gdc_pi_inst.get(pi_name, 'unknown')
                    default_suffix = inst
                    if first_conflict:
                        input("\n--- If nothing appears bellow press enter again, if text bellow then ignore this message ")
                        first_conflict = False
                    print(f"\nPI '{pi_name}' already exists (institution: {existing_inst})", flush=True)
                    print(f"New PI institution: {inst}", flush=True)
                    print(f"Options:", flush=True)
                    print(f"  Enter      = bundle with existing (no new entry)", flush=True)
                    print(f"  SUFFIX     = create new entry as '{pi_name} (SUFFIX)'", flush=True)
                    print(f"  m.INST     = merge & update existing institution to INST", flush=True)
                    user_input = input(f"[{default_suffix}]: ").strip()
                    if user_input.startswith('m.'):
                        # Merge with existing and update institution
                        new_inst = user_input[2:]
                        db.query(f"UPDATE pi_info SET pi_institution = '{sql_escape(new_inst)}' WHERE pi_name = '{sql_escape(pi_name)}'")
                        print(f"  → Updated '{pi_name}' institution to '{new_inst}'")
                        pi_name_remap[pi_name] = pi_name
                        continue  # Skip inserting this PI
                    elif user_input:
                        insert_name = f"{pi_name} ({user_input})"
                        pi_name_remap[pi_name] = insert_name
                    else:
                        # Bundle with existing - don't insert, just map to existing
                        pi_name_remap[pi_name] = pi_name
                        continue  # Skip inserting this PI
                gdc_names.add(insert_name)

                full = sql_escape(insert_name)
                inst_escaped = sql_escape(inst)
                pi_values.append(f"('{full}', '{first}', '{last}', '{inst_escaped}', {r2r_id}, '{r2r_inst}')")

            pi_sql = "INSERT INTO pi_info (pi_name, pi_first_name, pi_last_name, pi_institution, pi_r2r_id, pi_r2r_institution) VALUES\n"
            pi_sql += ',\n'.join(pi_values) + ';'
            db.query(pi_sql)
            print(f"✓ Inserted {len(missing_pis)} PIs", flush=True)

        # ===== INSERT Ports =====
        if missing_ports:
            port_values = []
            for port_name in missing_ports:
                parts = port_name.rsplit(', ', 1)
                r2r_name = parts[0] if len(parts) > 1 else port_name
                vocab_data = port_vocab.get(r2r_name, {})

                safe_name = sql_escape(port_name)
                lat = sql_val(vocab_data.get('latitude'))
                lon = sql_val(vocab_data.get('longitude'))
                r2r_id = vocab_data.get('r2r_id', '')
                country_code = (vocab_data.get('country_id3') or '').lower()
                country = sql_escape(country_map.get(country_code, country_code.upper()))
                port_values.append(f"('{safe_name}', '{r2r_id}', {lat}, {lon}, '{country}')")

            port_sql = "INSERT INTO port_info (port_name, r2r_id, latitude, longitude, port_country) VALUES\n"
            port_sql += ',\n'.join(port_values) + ';'
            db.query(port_sql)
            print(f"✓ Inserted {len(missing_ports)} Ports", flush=True)

        # Get FK IDs
        pi_id_map = gdc_pi_map.copy()
        pi_id_to_name = {}  # id -> pi_name (for FK lookup)
        port_id_map = {}

        # Build reverse map from existing GDC PIs
        if all_pis:
            for row in all_gdc_pis:
                pi_id_to_name[row['id']] = row['pi_name']

        if missing_pis:
            # Query by inserted names (may include renamed ones with suffixes)
            inserted_names = [pi_name_remap.get(n, n) for n in missing_pis.keys()]
            pi_results = db.query(f"SELECT id, pi_name FROM pi_info WHERE pi_name IN ('{sql_list(inserted_names)}')")
            for row in pi_results:
                _, normalized, _, _ = parse_pi_name(row['pi_name'])
                pi_id_map[row['pi_name']] = row['id']
                pi_id_map[normalized] = row['id']
                pi_id_to_name[row['id']] = row['pi_name']
            # Map original names to IDs for renamed PIs
            for orig, renamed in pi_name_remap.items():
                if renamed in pi_id_map:
                    pi_id_map[orig] = pi_id_map[renamed]

        if all_ports:
            port_results = db.query(f"SELECT id, port_name FROM port_info WHERE port_name IN ('{sql_list(all_ports)}')")
            for row in port_results:
                port_id_map[row['port_name']] = row['id']

        # Insert cruises
        cruise_values = []
        for cruise_id, cruise_data in missing_cruises_map.items():
            safe_id = sql_escape(cruise_id)
            title = sql_escape(cruise_data.get('cruise_name'))
            vessel = sql_escape(cruise_data.get('vessel'))
            start = cruise_data.get('depart_date')
            end = cruise_data.get('arrive_date')

            raw_pi_name = cruise_data.get('chief_scientist') or ''
            original_pi, normalized_pi, _, _ = parse_pi_name(raw_pi_name) if raw_pi_name else ('', '', '', '')

            raw_port_start = cruise_data.get('depart_port') or ''
            raw_port_end = cruise_data.get('arrive_port') or ''
            normalized_port_start = normalize_port_name(raw_port_start, port_vocab) if raw_port_start else ''
            normalized_port_end = normalize_port_name(raw_port_end, port_vocab) if raw_port_end else ''

            pi_id = 'NULL' if not original_pi else (pi_id_map.get(original_pi) or pi_id_map.get(normalized_pi, 'NULL'))
            # Use GDC pi_name (for FK), not R2R name
            if pi_id != 'NULL':
                gdc_pi_name = pi_id_to_name.get(pi_id, '')
                pi_name_text = f"'{sql_escape(gdc_pi_name)}'" if gdc_pi_name else 'NULL'
            else:
                pi_name_text = 'NULL'
            port_start_id = port_id_map.get(normalized_port_start, 'NULL') if normalized_port_start else 'NULL'
            port_end_id = port_id_map.get(normalized_port_end, 'NULL') if normalized_port_end else 'NULL'

            port_start_text = sql_escape(normalized_port_start)
            port_end_text = sql_escape(normalized_port_end)

            lat_min = sql_val(cruise_data.get('latitude_min'))
            lat_max = sql_val(cruise_data.get('latitude_max'))
            lon_min = sql_val(cruise_data.get('longitude_min'))
            lon_max = sql_val(cruise_data.get('longitude_max'))

            inst = sql_escape(cruise_data.get('operator_name'))
            start_str = f"'{start}'" if start else 'NULL'
            end_str = f"'{end}'" if end else 'NULL'

            cruise_values.append(f"('{safe_id}', '{title}', '{vessel}', {start_str}, {end_str}, {pi_id}, {pi_name_text}, {port_start_id}, {port_end_id}, '{port_start_text}', '{port_end_text}', {lat_min}, {lat_max}, {lon_min}, {lon_max}, '{inst}')")

        cruise_sql = "INSERT INTO cruise_info (cruise_id, cruise_title, vessel, cruise_start_date, cruise_end_date, pi_db_id, pi_name, port_start_db_id, port_end_db_id, port_start, port_end, latitude_minimum, latitude_maximum, longitude_minimum, longitude_maximum, institution) VALUES\n"
        cruise_sql += ',\n'.join(cruise_values) + ';'
        db.query(cruise_sql)

        print(f"\n✓ Inserted: {len(missing_pis)} PIs, {len(missing_ports)} Ports, {len(missing_cruises_map)} Cruises ({time.time() - start_time:.2f}s)\n")

if __name__ == "__main__":
    main()
