import io
import uuid
import pandas as pd
import json
import httpx
import contextlib
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify, Response, stream_with_context
from schemas import KoboUpdateSchema

clone_bp = Blueprint('clone_bp', __name__)

def set_nested_value(data_dict, path, value):
    keys = path.split('/')
    for key in keys[:-1]:
        data_dict = data_dict.setdefault(key, {})
    data_dict[keys[-1]] = value

@clone_bp.route('/clone', methods=['POST'])
def clone():
    try:
        # 1. Config Validation
        raw_url = request.form.get('server_url', '').rstrip('/')
        is_private_kc = "kobo-kc" in raw_url or "savethechildren" in raw_url
        
        raw_data = {
            "server_url": raw_url,
            "token": request.form.get('token'),
            "asset_id": request.form.get('target_asset_id')
        }
        
        config = KoboUpdateSchema(**raw_data)
        csv_file = request.files.get('file')

        # 2. CSV Reading and Basic Validation
        try:
            df = pd.read_csv(io.BytesIO(csv_file.read()), encoding='utf-8-sig', sep=None, engine='python')
            if df.empty:
                return jsonify({"status": "error", "message": "CSV file is empty."}), 400
        except Exception as e:
            return jsonify({"status": "error", "message": f"CSV Read Error: {str(e)}"}), 400

        headers = {
            "Authorization": f"Token {config.token}",
            "Accept": "application/json"
        }
        
        # 3. Initial Schema Fetch
        with httpx.Client(headers=headers, timeout=30.0) as client:
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
            
            # Map choice list names to allowed values
            choice_map = {}
            for c in choices_list:
                list_name = c.get('list_name')
                if list_name not in choice_map: choice_map[list_name] = []
                choice_map[list_name].append(str(c.get('name')))
                
            # Path Mapping Logic
            path_map = {}
            field_constraints = {}
            field_types = {}
            group_stack = []
            excluded_types = ['begin_group', 'end_group', 'calculate', 'note', 'deviceid']
            has_start = any(i.get('type') == 'start' for i in survey)
            has_end = any(i.get('type') == 'end' for i in survey)

            for item in survey:
                i_type, i_name = item.get('type'), item.get('name')
                if i_type == 'begin_group':
                    group_stack.append(i_name)
                elif i_type == 'end_group':
                    if group_stack: group_stack.pop()
                elif i_type not in excluded_types and i_name:
                    full_path = "/".join(group_stack + [i_name])
                    path_map[i_name.lower()] = full_path
                    path_map[full_path.lower()] = full_path
                    field_types[full_path] = i_type
                    
                    if i_type in ['select_one', 'select_multiple']:
                        list_name = item.get('select_from_list_name')
                        field_constraints[full_path] = {
                            "type": i_type,
                            "allowed": choice_map.get(list_name, [])
                        }

        valid_fields = {col: path_map[str(col).strip().lower()] 
                    for col in df.columns if str(col).strip().lower() in path_map}

        # 4. Generator Function for Streaming
        def generate():
            success_count = 0
            duplicate_id_count = 0
            invalid_id_count = 0
            processed_count = 0
            total_rows = len(df)
            has_id_column = '_id' in df.columns
            
            base_api_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/data/"
            submit_url = f"{config.server_url}/submission" if is_private_kc else f"{config.server_url}/api/v2/assets/{config.asset_id}/submissions/"
            
            with httpx.Client(headers=headers, timeout=120.0, follow_redirects=True) as client:
                # Pre-fetch existing IDs
                existing_ids = set()
                if has_id_column:
                    with contextlib.suppress(Exception):
                        list_resp = client.get(f"{base_api_url}?fields=[\"_id\"]&limit=10000")
                        if list_resp.status_code == 200:
                            data = list_resp.json()
                            existing_ids = {str(item['_id']) for item in data.get('results', [])}
                
                for _, row in df.iterrows():
                    processed_count += 1
                    try:
                        # 1. ID VALIDATION (If applicable)
                        if has_id_column:
                            raw_id = row.get('_id')
                            sub_id = str(raw_id).split('.')[0].strip() if pd.notna(raw_id) else ""
                            
                            # 1.1 EMPTY CHECK
                            if not sub_id or sub_id == 'nan':
                                invalid_id_count += 1
                                raise ValueError("ID is empty.")
                        
                            # 1.2 SCHEMA VALIDATION (Structural Check)
                            try:
                                KoboUpdateSchema.validate_kobo_id(sub_id)
                            except ValueError as schema_err:
                                invalid_id_count += 1
                                raise ValueError(f"{str(schema_err)}")
                            
                            # 1.3 CHECK FOR DUPLICATES
                            if sub_id in existing_ids:
                                duplicate_id_count +=1
                                raise ValueError(f"ID {sub_id} already exists in Kobo.")
                            
                        # 2. DATA PREPARATION & CHOICE VALIDATION
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
                            
                            # Validate Choices
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
                                    str_val = " ".join(selected_items)
                            
                            data_payload[xml_path] = str_val

                        if not data_payload: continue
                        
                        # 3. BUILD NESTED STRUCTURE
                        submission_data = {}
                        for path, clean_val in data_payload.items():
                            set_nested_value(submission_data, path, clean_val)
                        
                        # 4. ADD META & TIMESTAMPS
                        now_utc = datetime.now(timezone.utc).isoformat()
                        if has_start: submission_data["start"] = now_utc
                        if has_end: submission_data["end"] = now_utc
                        submission_data.setdefault("meta", {})["instanceID"] = f"uuid:{uuid.uuid4()}"
                        
                    except ValueError as e:
                        yield json.dumps({
                            "status": "warning", 
                            "message": str(e)
                        }) + "\n"
                        continue
                    
                    # Build Submission Data
                    submission_data = {}
                    for csv_col, xml_path in valid_fields.items():
                        val = row[csv_col]
                        if pd.notna(val):
                            set_nested_value(submission_data, xml_path, str(val))
                    
                    if not submission_data:
                        continue

                    now_utc = datetime.now(timezone.utc).isoformat()
                    if has_start: submission_data["start"] = now_utc
                    if has_end: submission_data["end"] = now_utc

                    payload = {
                        "id": config.asset_id,
                        "submission": {**submission_data, "meta": {"instanceID": f"uuid:{uuid.uuid4()}"}}
                    } if is_private_kc else submission_data

                    try:
                        resp = client.post(submit_url, json=payload)
                        if resp.status_code in [200, 201, 202]:
                            success_count += 1
                        else:
                            yield json.dumps({
                                "status": "error", 
                                "message": f"Row {processed_count} failed server-side: {resp.text[:100]}"
                            }) + "\n"
                    except Exception as e:
                        yield json.dumps({
                            "status": "error", 
                            "message": f"System error on row {processed_count}: {str(e)}"
                        }) + "\n"
                        continue
                    
                    yield json.dumps({
                        "status": "progress", 
                        "current": processed_count, 
                        "total": total_rows,
                        "success": success_count
                    }) + "\n"

                yield json.dumps({
                    "status": "success", 
                    "message": f"Successfully cloned {success_count} records. Skipped {duplicate_id_count} duplicated _id, {invalid_id_count} invalid _id."
                }) + "\n"

        return Response(
            stream_with_context(generate()), 
            content_type='application/x-ndjson'
        )

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500