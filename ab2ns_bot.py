from aiohttp import web
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext
from botbuilder.schema import Activity
import pandas as pd
import os
import tempfile
import requests

# Bot settings
APP_ID = "67b80685-7f25-4ade-b5bc-fa19495359fb"  # Replace with Azure Bot App ID
APP_PASSWORD = "00f86acc-fbed-42b9-ba11-92512dea3040"  # Replace with Azure Bot App Password

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

async def process_message(context: TurnContext):
    user_message = context.activity.text.strip().lower()

    if user_message == "hello bot":
        # Respond to "hello bot"
        await context.send_activity("Please upload your Alberta sales file in TXT format.")
    elif user_message == "convert file":
        # Respond to "convert file" command
        await context.send_activity("Please upload your .txt file.")
    elif context.activity.attachments:
        # Handle file attachments
        for attachment in context.activity.attachments:
            await context.send_activity("Processing your uploaded file...")
            # Logic for file processing here
            processed_file = "path_to_processed_file.csv"

            # Send back the converted file
            await context.send_activity(
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
        # Default response for unknown commands
        await context.send_activity("I'm sorry, I didn't understand that. Type 'hello bot' or 'convert file' to start.")


async def messages(req: web.Request) -> web.Response:
    body = await req.json()
    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")
    response = web.Response()

    async def aux_func(turn_context):
        await process_message(turn_context)

    await adapter.process_activity(activity, auth_header, aux_func)
    return response

# Create the web app
app = web.Application()
app.router.add_post("/api/messages", messages)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=int(os.environ.get("PORT", 3978)))
