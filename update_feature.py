import io
import json
import pandas as pd
import httpx, truststore, ssl
from flask import Blueprint, request, jsonify, Response, stream_with_context
import contextlib
from schemas import KoboUpdateSchema

update_bp = Blueprint('update_bp', __name__)

@update_bp.route('/update', methods=['POST'])
def update():
    try:
        # 1. Config Validation
        raw_data = {
            "server_url": request.form.get('server_url', '').rstrip('/'),
            "token": request.form.get('token'),
            "asset_id": request.form.get('asset_id')
        }
        config = KoboUpdateSchema(**raw_data)
        csv_file = request.files.get('file')

        # 2. CSV Reading
        try:
            df = pd.read_csv(io.BytesIO(csv_file.read()), encoding='utf-8-sig', sep=None, engine='python')
            if df.empty:
                return jsonify({"status": "error", "message": "CSV file is empty."}), 400
            if '_id' not in df.columns:
                return jsonify({"status": "error", "message": "CSV missing '_id' column."}), 400
        except Exception as e:
            return jsonify({"status": "error", "message": f"CSV Read Error: {str(e)}"}), 400

        headers = {"Authorization": f"Token {config.token}"}
        ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT) 
        
        # FETCH SCHEMA
        with httpx.Client(headers=headers, timeout=30.0, follow_redirects=True, verify=ctx) as client:
            verify_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/"
            try:
                auth_resp = client.get(verify_url)
                if auth_resp.status_code == 401:
                    return jsonify({"status": "error", "message": "Invalid API Token."}), 401
                if auth_resp.status_code == 404:
                    return jsonify({"status": "error", "message": "Asset ID not found on this server."}), 404
                
                survey = auth_resp.json().get('content', {}).get('survey', [])
                choices_list = auth_resp.json().get('content', {}).get('choices', [])
                
            except httpx.ConnectError:
                return jsonify({"status": "error", "message": "Could not connect to server. Check your Server URL."}), 400
            
            # Map choice list names to their valid values
            choice_map = {}
            for c in choices_list:
                list_name = c.get('list_name')
                if list_name not in choice_map: choice_map[list_name] = []
                choice_map[list_name].append(str(c.get('name')))

            # Path Mapping Logic
            path_map = {}
            field_types = {}
            field_constraints = {}
            group_stack = []
            excluded_types = ['begin_group', 'end_group', 'calculate', 'start', 'end', 'note', 'deviceid']

            for item in survey:
                i_type, i_name = item.get('type'), item.get('name')
                if i_type == 'begin_group':
                    group_stack.append(i_name)
                    
                elif i_type == 'end_group':
                    if group_stack: group_stack.pop()
                    
                elif i_type not in excluded_types and i_name:
                    full_path = "/".join(group_stack + [i_name])
                    path_map[full_path.lower()] = full_path
                    field_types[full_path] = i_type
                    
                    if i_type in ['select_one', 'select_multiple']:
                        list_name = item.get('select_from_list_name')
                        field_constraints[full_path] = {
                            "type": i_type,
                            "allowed": choice_map.get(list_name, [])
                        }

            valid_fields = {col: path_map[col.lower()] for col in df.columns if col.lower() in path_map}

            # Pre-fetch existing IDs
            existing_ids = set()
            with contextlib.suppress(Exception):
                base_api_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/data"
                list_resp = client.get(f"{base_api_url}/?fields=[\"_id\"]&limit=10000")
                if list_resp.status_code == 200:
                    existing_ids = {str(item['_id']) for item in list_resp.json().get('results', [])}

        # 4. Generator Function for Streaming Progress
        def generate():
            updated_count = 0
            processed_count = 0
            invalid_ids_count = 0
            not_found_count = 0
            total_rows = len(df)
            patch_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/data/bulk/"

            with httpx.Client(headers=headers, timeout=120.0, follow_redirects=True, verify=ctx) as client:
                for _, row in df.iterrows():
                    processed_count += 1
                    raw_val = row.get('_id')
                    
                    # ID Cleaning: Convert to float, then int, then string to remove .0
                    sub_id = str(int(float(raw_val))).strip() if pd.notna(raw_val) and str(raw_val).lower() != 'nan' else ""
                    
                    try:
                        # 1. EMPTY CHECK
                        if not sub_id or sub_id == 'nan':
                            invalid_ids_count += 1
                            raise ValueError("ID is empty.")
                        
                        # 2. SCHEMA VALIDATION (Structural Check)
                        try:
                            KoboUpdateSchema.validate_kobo_id(sub_id)
                        except ValueError as schema_err:
                            invalid_ids_count += 1
                            raise ValueError(f"{str(schema_err)}")
                        
                        # 3. EXISTENCE CHECK (Not Found)
                        if sub_id not in existing_ids:
                            not_found_count += 1
                            raise ValueError(f"ID {sub_id} not found in Kobo.")
                        
                        # 4. CHOICES VALIDATION LOGIC
                        data_payload = {}
                        for csv_col, xml_path in valid_fields.items():
                            val = row[csv_col]
                            if pd.isna(val): continue
                            
                            str_val = str(val).strip()
                            
                            # Get the expected type from our map
                            expected_type = field_types.get(xml_path, 'text')

                            # A. Validate Integers
                            if expected_type == 'integer':
                                try:
                                    # Check if it's a valid whole number
                                    float_val = float(str_val)
                                    if not float_val.is_integer():
                                        raise ValueError
                                    str_val = str(int(float_val)) # Normalize (e.g., "10.0" -> "10")
                                except ValueError:
                                    raise ValueError(f"Column '{csv_col}' expects a whole number, but got '{str_val}'.")

                            # B. Validate Decimals
                            elif expected_type == 'decimal':
                                try:
                                    float(str_val)
                                except ValueError:
                                    raise ValueError(f"Column '{csv_col}' expects a decimal number, but got '{str_val}'.")

                            # C. Validate Dates (YYYY-MM-DD)
                            elif expected_type == 'date':
                                try:
                                    # Kobo expects ISO format YYYY-MM-DD
                                    pd.to_datetime(str_val).strftime('%Y-%m-%d')
                                except:
                                    raise ValueError(f"Column '{csv_col}' expects a date, but '{str_val}' is invalid.")
                            
                            # Check if this field has Kobo constraints (Select One/Multi)
                            if xml_path in field_constraints:
                                constraint = field_constraints[xml_path]
                                allowed = constraint["allowed"]
                                
                                if constraint["type"] == "select_one":
                                    if str_val not in allowed:
                                        raise ValueError(f"'{str_val}' is not a valid choice for '{csv_col}'. Allowed: {allowed}")
                                
                                elif constraint["type"] == "select_multiple":
                                    # Split by space (Kobo format) or comma (Common CSV format)
                                    selected_items = [s.strip() for s in str_val.replace(',', ' ').split()]
                                    for item in selected_items:
                                        if item not in allowed:
                                            raise ValueError(f"'{item}' in '{str_val}' is not a valid choice for '{csv_col}'. Allowed: {allowed}")
                                    str_val = " ".join(selected_items) # Ensure Kobo space-separated format
                                    
                            data_payload[xml_path] = str_val
                        
                    except ValueError as e:
                        yield json.dumps({
                            "status": "warning", 
                            "message": str(e)
                        }) + "\n"
                        continue
                    
                    # Send Update
                    bulk_payload = {
                        "payload": {
                            "submission_ids": [int(float(sub_id))],
                            "data": data_payload
                        }
                    }

                    try:
                        resp = client.patch(patch_url, json=bulk_payload)
                        if resp.status_code in [200, 201]:
                            updated_count += 1
                        else:
                            yield json.dumps({"status": "error", "message": f"ID {sub_id} failed: {resp.text[:100]}"}) + "\n"
                    except Exception as e:
                        yield json.dumps({"status": "error", "message": f"System error on ID {sub_id}: {str(e)}"}) + "\n"
                    
                    # Yield progress update
                    yield json.dumps({
                        "status": "progress",
                        "current": processed_count,
                        "total": total_rows,
                        "success": updated_count
                    }) + "\n"

                # Yield final result
                yield json.dumps({
                    "status": "success",
                    "message": f"Update complete: {updated_count} updated, {invalid_ids_count} invalid _ids, {not_found_count} _ids not found."
                }) + "\n"

        return Response(
            stream_with_context(generate()), 
            content_type='application/x-ndjson'
        )

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500