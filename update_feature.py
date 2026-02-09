import io
import json
import asyncio
import pandas as pd
import httpx
from flask import Blueprint, request, jsonify, Response, stream_with_context
from schemas import KoboUpdateSchema

update_bp = Blueprint('update_bp', __name__)

@update_bp.route('/update', methods=['POST'])
async def update():
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
        
        # 3. Initial Schema Fetch
        async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
            verify_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/"
            auth_resp = await client.get(verify_url)
            if auth_resp.status_code == 401:
                return jsonify({"status": "error", "message": "Invalid API Token."}), 401
            if auth_resp.status_code == 404:
                return jsonify({"status": "error", "message": "Asset ID not found."}), 404
            if httpx.ConnectError:
                return jsonify({"status": "error", "message": "Could not connect to server. Check your Server URL."}), 400
            
            survey = auth_resp.json().get('content', {}).get('survey', [])

            path_map = {}
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

            valid_fields = {col: path_map[col.lower()] for col in df.columns if col.lower() in path_map}

        # 4. Generator Function with Batching Logic
        async def generate():
            updated_count = 0
            processed_count = 0
            invalid_ids_count = 0
            not_found_count = 0
            total_rows = len(df)
            patch_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/data/bulk/"
            
            async with httpx.AsyncClient(headers=headers, timeout=120.0) as client:
                # Pre-fetch existing IDs
                base_api_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/data"
                existing_ids = set()
                try:
                    list_resp = await client.get(base_api_url, params={"fields": '["_id"]', "limit": 10000})
                    if list_resp.status_code == 200:
                        existing_ids = {str(item['_id']) for item in list_resp.json().get('results', [])}
                except Exception as e:
                    yield json.dumps({"status": "warning", "message": f"Pre-fetch error: {str(e)}"}) + "\n"

                # Define the processing logic for a single row
                async def process_single_row(row):
                    nonlocal updated_count, invalid_ids_count, not_found_count
                    raw_val = row.get('_id')
                    sub_id = str(raw_val).split('.')[0].strip() if pd.notna(raw_val) else ""

                    try:
                        if not sub_id or sub_id == 'nan':
                            invalid_ids_count += 1
                            raise ValueError("ID is empty.")
                        
                        KoboUpdateSchema.validate_kobo_id(sub_id)
                        
                        if sub_id not in existing_ids:
                            not_found_count += 1
                            raise ValueError(f"ID {sub_id} not found in Kobo.")

                        data_payload = {xml_path: str(row[csv_col]) for csv_col, xml_path in valid_fields.items() if pd.notna(row[csv_col])}
                        if data_payload:
                            bulk_payload = {"payload": {"submission_ids": [int(float(sub_id))], "data": data_payload}}
                            resp = await client.patch(patch_url, json=bulk_payload)
                            if resp.status_code in [200, 201]:
                                updated_count += 1
                                return None # Success
                            else:
                                return json.dumps({"status": "error", "message": f"ID {sub_id} failed: {resp.text[:50]}"}) + "\n"
                    except ValueError as e:
                        return json.dumps({"status": "warning", "message": str(e)}) + "\n"
                    except Exception as e:
                        return json.dumps({"status": "warning", "message": f"Network error on ID {sub_id}: {str(e)}"}) + "\n"
                    return None

                # Process in Batches of 5
                batch_size = 5
                for i in range(0, total_rows, batch_size):
                    batch = df.iloc[i : i + batch_size]
                    # Fire all requests in this batch at once
                    tasks = [process_single_row(row) for _, row in batch.iterrows()]
                    results = await asyncio.gather(*tasks)
                    
                    processed_count += len(batch)

                    # Yield errors/warnings from the batch if any
                    for error_msg in results:
                        if error_msg:
                            yield error_msg

                    # Yield progress update
                    yield json.dumps({
                        "status": "progress",
                        "current": processed_count,
                        "total": total_rows,
                        "success": updated_count
                    }) + "\n"

                yield json.dumps({
                    "status": "success",
                    "message": f"Update complete: {updated_count} updated, {invalid_ids_count} invalid, {not_found_count} not found."
                }) + "\n"

        # 5. Sync wrapper
        def sync_wrapper(async_gen):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                while True:
                    try:
                        yield loop.run_until_complete(async_gen.__anext__())
                    except StopAsyncIteration:
                        break
            finally:
                loop.close()

        return Response(stream_with_context(sync_wrapper(generate())), content_type='application/x-ndjson')

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500