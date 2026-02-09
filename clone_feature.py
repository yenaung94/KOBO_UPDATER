import io
import uuid
import pandas as pd
import json
import httpx
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
async def clone():
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
        
        # 3. Initial Schema Fetch (To validate Asset and get Mapping)
        async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
            verify_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/"
            try:
                auth_resp = await client.get(verify_url)
                if auth_resp.status_code == 401:
                    return jsonify({"status": "error", "message": "Invalid API Token."}), 401
                if auth_resp.status_code == 404:
                    return jsonify({"status": "error", "message": "Asset ID not found on this server."}), 404
                
                survey = auth_resp.json().get('content', {}).get('survey', [])
            except httpx.ConnectError:
                return jsonify({"status": "error", "message": "Could not connect to server. Check your Server URL."}), 400

        # Path Mapping Logic (Extracted from survey)
        path_map = {}
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

        valid_fields = {col: path_map[str(col).strip().lower()] 
                       for col in df.columns if str(col).strip().lower() in path_map}

        # 4. Generator Function for Streaming
        async def generate():
            success_count = 0
            skipped_count = 0
            processed_count = 0
            total_rows = len(df)
            has_id_column = '_id' in df.columns
            
            base_api_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/data"
            submit_url = f"{config.server_url}/submission" if is_private_kc else f"{config.server_url}/api/v2/assets/{config.asset_id}/submissions/"
            
            # Open client inside the generator
            async with httpx.AsyncClient(headers=headers, timeout=120.0) as client:
                existing_ids = set()
                if has_id_column:
                    try:
                        # We only need the '_id' field from Kobo
                        list_resp = await client.get(f"{base_api_url}/?fields=[\"_id\"]&limit=10000")
                        if list_resp.status_code == 200:
                            data = list_resp.json()
                            existing_ids = {str(item['_id']) for item in data.get('results', [])}
                    except Exception : pass
                    
                for _, row in df.iterrows():
                    processed_count += 1
                    if has_id_column:
                        raw_id = row.get('_id')
                        sub_id = str(raw_id).split('.')[0].strip() if pd.notna(raw_id) else ""
                        
                        # 2. Duplicate/Validation Check
                        try:
                            KoboUpdateSchema.validate_kobo_id(sub_id)
                            if sub_id in existing_ids:
                                raise ValueError(f"ID {sub_id} already exists.")

                        except ValueError as e:
                            skipped_count += 1
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
                        resp = await client.post(submit_url, json=payload)
                        if resp.status_code in [200, 201, 202]:
                            success_count += 1
                        else:
                            yield json.dumps({
                                "status": "error", 
                                "message": f"Row {processed_count} failed server-side: {resp.text[:100]}"
                            }) + "\n"
                    except Exception as e:
                        continue
                    
                    yield json.dumps({
                        "status": "progress", 
                        "current": processed_count, 
                        "total": total_rows,
                        "success": success_count
                    }) + "\n"

                yield json.dumps({
                    "status": "success", 
                    "message": f"Successfully cloned {success_count} records. Skipped {skipped_count} duplicates/invalid."
                }) + "\n"

        import asyncio

        def sync_generator_wrapper(async_gen):
            loop = asyncio.new_event_loop()
            try:
                while True:
                    try:
                        yield loop.run_until_complete(async_gen.__anext__())
                    except StopAsyncIteration:
                        break
            finally:
                loop.close()

        return Response(
            stream_with_context(sync_generator_wrapper(generate())), 
            content_type='application/x-ndjson'
        )

    except Exception as e:
        print(f"CRITICAL: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500