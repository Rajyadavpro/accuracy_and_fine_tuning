import azure.functions as func
import logging
import os

# 1. Import the 'main' functions from your 3 script files, renaming them to avoid conflicts
from accuracy.idp_accuarcy import main as run_idp_accuracy
from accuracy.tabak_accuarcy import main as run_tabak_accuracy
from accuracy.healthcare_eob_accuracy import main as run_healthcare_eob_accuracy
from accuracy.healthcare_accuracy import main as run_healthcare_accuracy
from accuracy.healthcare_superbill_accuracy import main as run_healthcare_superbill_accuracy

# Keep your existing imports for tasks 4, 5, 6
from fine_tuning.tabak_fine_tuning_data_get import tabak_fine_tuning_data_push
from fine_tuning.eob_fine_tuning_data_get import eob_fine_tuning_data_push
from fine_tuning.superbill_fine_tuning_data_get import superbill_fine_tuning_data_push
from fine_tuning.idp_fine_tuning_data_get import idp_fine_tuning_data_push as run_idp_fine_tuning_data_push
app = func.FunctionApp()

# ==========================================
# TIMER TRIGGER ENTRY POINT
# ==========================================
@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def main_timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due.')
    logging.info('Timer trigger activated. Running all 3 associated tasks...')
    logging.info('All timer tasks completed.')

# ==========================================
# HTTP TRIGGER ENTRY POINT (WITH TEST PARAMS)
# ==========================================
@app.route(route="run_http_tasks", auth_level=func.AuthLevel.ANONYMOUS)
def main_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP trigger activated. Evaluating parameters...')
    
    # Parse 'task_id' and optional params
    task_id = req.params.get('task_id')
    ids_per_message_raw = req.params.get('ids_per_message')
    max_messages_raw = req.params.get('max_messages_per_run')
    
    body_data = {}
    try:
        body_data = req.get_json() or {}
    except ValueError:
        pass

    if not task_id:
        task_id = body_data.get('task_id')
    if not ids_per_message_raw:
        ids_per_message_raw = body_data.get('ids_per_message')
    if not max_messages_raw:
        max_messages_raw = body_data.get('max_messages_per_run')

    # Convert values to integers safely
    ids_per_message = None
    if ids_per_message_raw is not None:
        try:
            ids_per_message = int(ids_per_message_raw)
        except ValueError:
            logging.warning(f"Invalid integer value provided for ids_per_message: {ids_per_message_raw}")

    max_messages_per_run = None
    if max_messages_raw is not None:
        try:
            max_messages_per_run = int(max_messages_raw)
        except ValueError:
            logging.warning(f"Invalid integer value provided for max_messages_per_run: {max_messages_raw}")

    if not task_id:
        return func.HttpResponse(
            "Please provide a 'task_id' parameter in the query string or request body.",
            status_code=400
        )

    # 2. Execute selected function passing the test variables
    try:
        if task_id == '1':
            logging.info("Triggering IDP Accuracy script")
            # run_idp_accuracy(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run)  # Replaced idp_data_push() with your script
            run_idp_accuracy()
        elif task_id == '2':
            logging.info("Triggering Tabak Accuracy script")
            run_tabak_accuracy() # Replaced tabak_data_push() with your script

        elif task_id == '3':
            os.environ["HEALTHCARE_ACCURACY_FILE_TYPE"] = "eob"
            logging.info("Triggering Healthcare EOB Accuracy script")
            run_healthcare_eob_accuracy(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run)

        elif task_id == '4':
            os.environ["HEALTHCARE_ACCURACY_FILE_TYPE"] = "superbill"
            logging.info("Triggering Healthcare Superbill Accuracy script")
            run_healthcare_superbill_accuracy(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run)

        elif task_id == '5':
            logging.info(f"Triggering Tabak Fine Tuning data push with ids_per_message={ids_per_message}, max_messages_per_run={max_messages_per_run}")
            tabak_fine_tuning_data_push(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run)
            
        elif task_id == '6':
            logging.info(f"Triggering EOB Fine Tuning data push with ids_per_message={ids_per_message}, max_messages_per_run={max_messages_per_run}")
            eob_fine_tuning_data_push(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run)

        elif task_id == '7':
            logging.info(f"Triggering Superbill Fine Tuning data push with ids_per_message={ids_per_message}, max_messages_per_run={max_messages_per_run}")
            superbill_fine_tuning_data_push(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run)
            
        elif task_id == '8':
            os.environ.pop("HEALTHCARE_ACCURACY_FILE_TYPE", None)
            logging.info("Triggering Healthcare Accuracy script (both)")
            run_idp_fine_tuning_data_push(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run)
        else:
            return func.HttpResponse(
                "Please provide a valid 'task_id' parameter (from 1 to 8).",
                status_code=400
            )
            
        return func.HttpResponse(
            f"Successfully triggered HTTP task {task_id}.",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Error executing HTTP task: {e}")
        return func.HttpResponse(
            f"An error occurred while attempting to process the request: {e}",
            status_code=500
        )