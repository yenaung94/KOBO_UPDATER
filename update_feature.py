import io
import json
import pandas as pd
import httpx
from flask import Blueprint, request, jsonify, Response, stream_with_context
from schemas import KoboUpdateSchema

update_bp = Blueprint('update_bp', __name__)

@update_bp.route('/update', methods=['POST'])
def update():
    try:
        # 1️⃣ Config Validation
        raw_data = {
            "server_url": request.form.get('server_url', '').rstrip('/'),
            "token": request.form.get('token'),
            "asset_id": request.form.get('asset_id')
        }
        config = KoboUpdateSchema(**raw_data)
        csv_file = request.files.get('file')

        # 2️⃣ CSV Reading
        try:
            df = pd.read_csv(io.BytesIO(csv_file.read()), encoding='utf-8-sig', sep=",")  # Explicit separator
            if df.empty:
                return jsonify({"status": "error", "message": "CSV file is empty."}), 400
            if '_id' not in df.columns:
                return jsonify({"status": "error", "message": "CSV missing '_id' column."}), 400
        except Exception as e:
            return jsonify({"status": "error", "message": f"CSV Read Error: {str(e)}"}), 400

        headers = {"Authorization": f"Token {config.token}"}

        # 3️⃣ Fetch Survey Schema
        with httpx.Client(headers=headers, timeout=30) as client:
            verify_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/"
            try:
                auth_resp = request.get(verify_url, headers=headers, timeout=30)
                if auth_resp.status_code == 401:
                    return jsonify({"status": "error", "message": "Invalid API Token."}), 401
                if auth_resp.status_code == 404:
                    return jsonify({"status": "error", "message": "Asset ID not found."}), 404
                
                survey = auth_resp.json().get('content', {}).get('survey', [])
                
            except ConnectionError:
                return jsonify({"status": "error", "message": "Could not connect to server. Check your Server URL."}), 400

            survey = auth_resp.json().get('content', {}).get('survey', [])

            # Build path map
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

            # Pre-fetch existing IDs
            existing_ids = set()
            try:
                list_resp = client.get(f"{config.server_url}/api/v2/assets/{config.asset_id}/data",
                                       params={"fields": '["_id"]', "limit": 10000})
                if list_resp.status_code == 200:
                    existing_ids = {str(item['_id']) for item in list_resp.json().get('results', [])}
            except Exception as e:
                # We can continue even if prefetch fails
                existing_ids = set()

        # 4️⃣ Generator function to stream progress
        def generate():
            updated_count = 0
            invalid_ids_count = 0
            not_found_count = 0
            total_rows = len(df)
            batch_size = 5
            patch_url = f"{config.server_url}/api/v2/assets/{config.asset_id}/data/bulk/"

            with httpx.Client(headers=headers, timeout=120) as client:
                for i in range(0, total_rows, batch_size):
                    batch = df.iloc[i: i + batch_size]
                    for _, row in batch.iterrows():
                        raw_id = row.get('_id')
                        sub_id = str(raw_id).split('.')[0].strip() if pd.notna(raw_id) else ""

                        try:
                            if not sub_id or sub_id.lower() == 'nan':
                                invalid_ids_count += 1
                                yield json.dumps({"status": "warning", "message": "ID is empty."}) + "\n"
                                continue

                            KoboUpdateSchema.validate_kobo_id(sub_id)

                            if sub_id not in existing_ids:
                                not_found_count += 1
                                yield json.dumps({"status": "warning", "message": f"ID {sub_id} not found."}) + "\n"
                                continue

                            # Build payload
                            data_payload = {xml_path: str(row[csv_col])
                                            for csv_col, xml_path in valid_fields.items()
                                            if pd.notna(row[csv_col])}
                            if data_payload:
                                bulk_payload = {"payload": {"submission_ids": [int(float(sub_id))], "data": data_payload}}
                                resp = client.patch(patch_url, json=bulk_payload)
                                if resp.status_code in [200, 201]:
                                    updated_count += 1
                                else:
                                    yield json.dumps({"status": "error",
                                                      "message": f"ID {sub_id} failed: {resp.text[:50]}"}) + "\n"

                        except Exception as e:
                            yield json.dumps({"status": "warning",
                                              "message": f"Error on ID {sub_id}: {str(e)}"}) + "\n"

                    # Yield progress after each batch
                    yield json.dumps({
                        "status": "progress",
                        "current": min(i + batch_size, total_rows),
                        "total": total_rows,
                        "success": updated_count
                    }) + "\n"

            # Final summary
            yield json.dumps({
                "status": "success",
                "message": f"Update complete: {updated_count} updated, {invalid_ids_count} invalid, {not_found_count} not found."
            }) + "\n"

        return Response(stream_with_context(generate()), content_type='application/x-ndjson')

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
