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
        # FORM DATA VALIDATION
        raw_url = request.form.get('server_url', '').rstrip('/')
        is_confirmed_raw = request.form.get("is_confirmed", "false")
        is_confirmed = str(is_confirmed_raw).lower() == "true"
        skip_until = int(request.form.get('skip_until', 0))
        
        # 1. Config Validation
        raw_data = {
            "server_url": raw_url,
            "token": request.form.get('token'),
            "asset_id": request.form.get('asset_id')
        }
        config = KoboUpdateSchema(**raw_data)
        csv_file = request.files.get('file')

        # CSV READING AND VALIDATION
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
            
            # MAP CHOICES LIST NAMES TO ALLOWED VALUES
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

            csv_cols = [c.strip().lower() for c in df.columns if c.strip().lower() not in ['start', 'end', '_id', 'username']]
            invalid_cols = [c for c in csv_cols if c not in path_map]
            if invalid_cols:
                return jsonify({"status": "error", "message": f"Field Mismatch: {invalid_cols} are not in Kobo."}), 400
            
            valid_fields = {col: path_map[str(col).strip().lower()] for col in df.columns if str(col).strip().lower() in path_map}

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
            progress_count = 0
            invalid_ids_count = 0
            not_found_count = 0
            invalid_records_details = []
            
            total_rows = len(df)
            patch_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/data/bulk/"

            with httpx.Client(headers=headers, timeout=120.0, follow_redirects=True, verify=ctx) as client:
                for idx, row in df.iterrows():
                    processed_count = idx + 1
                    
                    is_valid = (not is_confirmed and processed_count == total_rows)
                    if not is_confirmed and processed_count <= skip_until:
                        continue
                    
                    raw_id = row.get('_id')
                    sub_id = str(raw_id).split('.')[0].strip() if pd.notna(raw_id) else ""
                    
                    try:
                        # 1. EMPTY CHECK
                        if not sub_id or sub_id.lower() in ['nan', 'null', 'none']:
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
                            raise ValueError("ID was not found in Kobo.")
                        
                        # 4. CHOICES VALIDATION LOGIC
                        data_payload = {}
                        for csv_col, xml_path in valid_fields.items():
                            val = row[csv_col]
                            if pd.isna(val): continue
                            
                            str_val = str(val).strip()
                            
                            # Get the expected type from our map
                            expected_type = field_types.get(xml_path, 'text')

                            # INTEGERS VALIDATION
                            if expected_type == 'integer':
                                try:
                                    # Check if it's a valid whole number
                                    float_val = float(str_val)
                                    if not float_val.is_integer():
                                        raise ValueError
                                    str_val = str(int(float_val)) # Normalize (e.g., "10.0" -> "10")
                                except ValueError:
                                    raise ValueError(f"Column '{csv_col}' expects a whole number, but got '{str_val}'.")

                            # DECIMALS VALIDATION
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
                            
                        if not data_payload:
                            yield json.dumps({
                                "status": "warning", 
                                "current": processed_count,
                                "total": total_rows,
                                "is_validation_complete": is_valid,
                                "message": f"Row {processed_count}: No matching Kobo fields found. Please check CSV headers."
                            })+ "\n"
                            return
                        
                    except ValueError as e:
                        raw_id_str = str(row.get('_id')).split('.')[0] if pd.notna(row.get('_id')) else "unknown"
                        error_msg = f"ID {raw_id_str}: {str(e)}"
                        invalid_records_details.append(error_msg)
                        
                        if not is_confirmed:
                            invalid_records_details.append(str(e))
                            yield json.dumps({
                                "status": "warning", 
                                "current": processed_count,
                                "total": total_rows,
                                "is_validation_complete": is_valid,
                                "message": f"Id_{raw_id_str}:  {str(e)}"
                            }) + "\n"
                            break
                        else:
                            continue
                    
                    if is_confirmed or is_valid:    
                        progress_count += 1
                        yield json.dumps({
                            "status": "progress", 
                            "total": total_rows,
                            "current" :progress_count,
                            "is_validation_complete": is_valid
                        }) + "\n"
                    
                    if is_confirmed:
                        bulk_payload = {
                            "payload": {"submission_ids": [int(float(sub_id))], "data": data_payload}
                        }
                        
                        resp = client.patch(patch_url, json=bulk_payload)
                        if resp.status_code in [200, 201]:
                            updated_count += 1

                not_found_part = f"{not_found_count} Id were not found. " if not_found_count > 0 else ""
                invalid_part = f"Found {invalid_ids_count} Id invalid records." if invalid_ids_count > 0 else ""
                yield json.dumps({
                    "status": "success",
                    "message": f"Update complete. Updated {updated_count} record(s). {not_found_part} {invalid_part}",
                    "err_detail" : invalid_records_details
                }) + "\n"
                
        return Response(
            stream_with_context(generate()), 
            content_type='application/x-ndjson'
        )

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500