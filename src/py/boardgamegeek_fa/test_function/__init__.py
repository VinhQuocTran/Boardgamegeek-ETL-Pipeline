import logging

import azure.functions as func
import os
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        # Load settings from local.settings.json
        with open("local.settings.json", "r") as settings_file:
            settings = json.load(settings_file)

            # Access specific settings
            connection_string = settings["Values"]["AzureWebJobsStorage"]

            # Use the settings in your Azure Function code
            print(f"AzureWebJobsStorage connection string: {connection_string}")
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
        

    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
