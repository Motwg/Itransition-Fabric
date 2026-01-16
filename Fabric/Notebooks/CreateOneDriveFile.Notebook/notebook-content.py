# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "07a74f6b-f276-b04e-491a-92665e2d7362",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

trips = '[ 		{ 			"vendor_id": 1, 			"pu_dt": "2025-05-01T00:00:00Z", 			"pu_hour": 4, 			"passengers": 1, 			"distance": 0, 			"ratecode_id": 1, 			"store_flag": "N", 			"pu_loc_id": 74, 			"do_loc_id": 168, 			"payment_type": 1, 			"total": 19, 			"fare": 17.5, 			"tips": 0, 			"extra": 0, 			"tax": 0.5, 			"tolls": 0, 			"improvement_surcharge": 1, 			"congestion_surcharge": 0, 			"airport_fee": 0, 			"congestion_fee": 0 		}, 		{ 			"vendor_id": 2, 			"pu_dt": "2025-05-01T00:00:00Z", 			"pu_hour": 4, 			"passengers": 1, 			"distance": 1.62, 			"ratecode_id": 1, 			"store_flag": "N", 			"pu_loc_id": 114, 			"do_loc_id": 261, 			"payment_type": 1, 			"total": 17, 			"fare": 9.3, 			"tips": 1.95, 			"extra": 1, 			"tax": 0.5, 			"tolls": 0, 			"improvement_surcharge": 1, 			"congestion_surcharge": 2.5, 			"airport_fee": 0, 			"congestion_fee": 0.75 		}, 		{ 			"vendor_id": 2, 			"pu_dt": "2025-05-01T00:00:00Z", 			"pu_hour": 5, 			"passengers": 5, 			"distance": 2.35, 			"ratecode_id": 1, 			"store_flag": "N", 			"pu_loc_id": 48, 			"do_loc_id": 238, 			"payment_type": 1, 			"total": 21.42, 			"fare": 12.1, 			"tips": 3.57, 			"extra": 1, 			"tax": 0.5, 			"tolls": 0, 			"improvement_surcharge": 1, 			"congestion_surcharge": 2.5, 			"airport_fee": 0, 			"congestion_fee": 0.75 		} 	] '
sensors = '[ 		{ 			"vendor_id": 1, 			"pu_dt": "2025-05-01T00:00:00Z", 			"pu_hour": 4, 			"passengers": 1, 			"distance": 0, 			"ratecode_id": 1, 			"store_flag": "N", 			"pu_loc_id": 74, 			"do_loc_id": 168, 			"payment_type": 1, 			"total": 19, 			"fare": 17.5, 			"tips": 0, 			"extra": 0, 			"tax": 0.5, 			"tolls": 0, 			"improvement_surcharge": 1, 			"congestion_surcharge": 0, 			"airport_fee": 0, 			"congestion_fee": 0 		}, 		{ 			"vendor_id": 2, 			"pu_dt": "2025-05-01T00:00:00Z", 			"pu_hour": 4, 			"passengers": 1, 			"distance": 1.62, 			"ratecode_id": 1, 			"store_flag": "N", 			"pu_loc_id": 114, 			"do_loc_id": 261, 			"payment_type": 1, 			"total": 17, 			"fare": 9.3, 			"tips": 1.95, 			"extra": 1, 			"tax": 0.5, 			"tolls": 0, 			"improvement_surcharge": 1, 			"congestion_surcharge": 2.5, 			"airport_fee": 0, 			"congestion_fee": 0.75 		}, 		{ 			"vendor_id": 2, 			"pu_dt": "2025-05-01T00:00:00Z", 			"pu_hour": 5, 			"passengers": 5, 			"distance": 2.35, 			"ratecode_id": 1, 			"store_flag": "N", 			"pu_loc_id": 48, 			"do_loc_id": 238, 			"payment_type": 1, 			"total": 21.42, 			"fare": 12.1, 			"tips": 3.57, 			"extra": 1, 			"tax": 0.5, 			"tolls": 0, 			"improvement_surcharge": 1, 			"congestion_surcharge": 2.5, 			"airport_fee": 0, 			"congestion_fee": 0.75 		} 	] '
dates = '[]'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

def format_data(s: str) -> str:
    return '\n'.join([f'{i + 1}: ' + '{' + ', '.join([f'{k}: {v}' for k, v in row.items()]) + '}' for i, row in enumerate(s)])

#trips_out = format_data(json.loads(trips))
#sensors_out = format_data(json.loads(sensors))
#dates_out = format_data(json.loads(dates))
#message = {'message': f'Trips: \n{trips_out}\n\nSensors: \n{sensors_out}\n\nDates: \n{dates_out}'}
message = {'message': {'Trips': json.loads(trips), 'Sensors': json.loads(sensors), 'Dates': json.loads(dates)}}

url = "https://defaultd57264d5c1c34928b4eb93f4da6bb2.6f.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/ebc9b8372fe24fb2a0f7140efd9b4a3f/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=d-M0O8SB4fyNJjoZniacj3gG3v0ppAQXEiUmUYSXSv4"
response = requests.post(url, json=message)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
