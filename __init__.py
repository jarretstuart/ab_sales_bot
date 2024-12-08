import logging
import azure.functions as func
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext
from botbuilder.schema import Activity
import pandas as pd
import os
import tempfile
import requests

# Bot settings
APP_ID = os.getenv("APP_ID")  # Fetch from environment variables
APP_PASSWORD = os.getenv("APP_PASSWORD")  # Fetch from environment variables

adapter_settings = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
adapter = BotFrameworkAdapter(adapter_settings)

# File transformation logic
def convert_and_process_txt_to_csv(txt_file_path, csv_file_path, headers, mapping_file):
    try:
        # Step 3: Read the .txt file with pandas
        df = pd.read_csv(
            txt_file_path, 
            header=None, 
            names=headers, 
            delimiter=',', 
            quotechar='"', 
            on_bad_lines='skip'
        )
        
        # Data transformations (same as your original logic)...
        df['Beginning of the week'] = df['Beginning of the week'].astype(str).str.replace(r'(\d{4})(\d{2})(\d{2})', r'\1-\2-\3', regex=True)
        df['End of the week'] = df['End of the week'].astype(str).str.replace(r'(\d{4})(\d{2})(\d{2})', r'\1-\2-\3', regex=True)
        df['ExternalID'] = "AB-" + df['End of the week']
        df['Order #'] = df['End of the week']
        df['Customer'] = "10000 AGLC"
        df['orderstatus'] = "Pending Fulfillment"
        df['location'] = "AB: Connect Logistics"
        df['Name'] = pd.to_numeric(df['Name'], errors='coerce').fillna(0).astype(int)
        
        mapping = pd.read_csv(mapping_file)
        mapping['Name'] = pd.to_numeric(mapping['Name'], errors='coerce').fillna(0).astype(int)
        mapping_dict = mapping.set_index('Name')['Internal ID'].to_dict()
        df['Internal ID'] = df['Name'].map(mapping_dict)
        
        df.to_csv(csv_file_path, index=False)
        return csv_file_path
    except Exception as e:
        return str(e)

async def process_message(turn_context: TurnContext):
    user_message = turn_context.activity.text.strip().lower()

    if user_message == "hello bot":
        await turn_context.send_activity("Please upload your Alberta sales file in TXT format.")
    elif user_message == "convert file":
        await turn_context.send_activity("Please upload your .txt file.")
    elif turn_context.activity.attachments:
        for attachment in turn_context.activity.attachments:
            await turn_context.send_activity("Processing your uploaded file...")
            # Logic for file processing here
            processed_file = "path_to_processed_file.csv"

            await turn_context.send_activity(
                "Here is your converted file.",
                attachments=[
                    {
                        "contentType": "application/vnd.ms-excel",
                        "contentUrl": f"https://yourserver/{processed_file}",
                        "name": "converted_file.csv"
                    }
                ]
            )
    else:
        await turn_context.send_activity("I'm sorry, I didn't understand that. Type 'hello bot' or 'convert file' to start.")

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Azure Function processed a request.")

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON payload.", status_code=400)

    activity = Activity.deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    async def aux_func(turn_context: TurnContext):
        await process_message(turn_context)

    await adapter.process_activity(activity, auth_header, aux_func)
    return func.HttpResponse("OK", status_code=200)
