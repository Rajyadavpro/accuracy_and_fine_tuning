import azure.functions as func
import logging
import os
import threading
from datetime import datetime

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
from fine_tuning.ccai_fine_tuning_data_get import audio_fine_tuning_data_push
app = func.FunctionApp()

# ==========================================
# TIMER TRIGGER ENTRY POINT
# ==========================================
@app.timer_trigger(schedule="0 0 14 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def main_timer_trigger(myTimer: func.TimerRequest) -> None:
    start_time = datetime.now()
    logging.info("=============================================================")
    logging.info(f"[Timer Trigger] Activated at {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logging.info("=============================================================")

    if myTimer.past_due:
        logging.warning('[Timer Trigger] Timer is past due — execution was delayed.')

    tasks = [
        ("Tabak Accuracy", run_tabak_accuracy, {}),
        ("IDP Accuracy", run_idp_accuracy, {}),
        ("Healthcare Superbill Accuracy", run_healthcare_superbill_accuracy, {}),
        ("Healthcare EOB Accuracy", run_healthcare_eob_accuracy, {}),
    ]

    for task_name, task_fn, kwargs in tasks:
        task_start = datetime.now()
        logging.info(f"[Timer Trigger] Starting: {task_name}...")
        try:
            task_fn(**kwargs)
            elapsed = (datetime.now() - task_start).total_seconds()
            logging.info(f"[Timer Trigger] Completed: {task_name} in {elapsed:.1f}s")
        except Exception as e:
            elapsed = (datetime.now() - task_start).total_seconds()
            logging.error(f"[Timer Trigger] FAILED: {task_name} after {elapsed:.1f}s — {e}", exc_info=True)

    total_elapsed = (datetime.now() - start_time).total_seconds()
    logging.info("=============================================================")
    logging.info(f"[Timer Trigger] All tasks finished in {total_elapsed:.1f}s")
    logging.info("=============================================================")
    
# ==========================================
# HTTP TRIGGER ENTRY POINT (WITH TEST PARAMS)
# ==========================================
@app.route(route="run_http_tasks", auth_level=func.AuthLevel.ANONYMOUS)
def main_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    from datetime import datetime
    
    logging.info('HTTP trigger activated. Evaluating parameters...')
    
    # Parse 'task_id' and optional params
    task_id = req.params.get('task_id')
    ids_per_message_raw = req.params.get('ids_per_message')
    max_messages_raw = req.params.get('max_messages_per_run')
    start_date = req.params.get('start_date')
    end_date = req.params.get('end_date')
    folder_name = req.params.get('folder_name')
    bypass_checkpoint_raw = req.params.get('bypass_checkpoint')
    
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
    if not start_date:
        start_date = body_data.get('start_date')
    if not end_date:
        end_date = body_data.get('end_date')
    if not folder_name:
        folder_name = body_data.get('folder_name')
    if not bypass_checkpoint_raw:
        bypass_checkpoint_raw = body_data.get('bypass_checkpoint')

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

    # Convert bypass_checkpoint to boolean
    bypass_checkpoint = False
    if bypass_checkpoint_raw is not None:
        bypass_checkpoint_str = str(bypass_checkpoint_raw).strip().lower()
        bypass_checkpoint = bypass_checkpoint_str in {'true', '1', 'yes', 'on'}

    # If end_date not provided, use current datetime
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"end_date not provided, using current datetime: {end_date}")


    if not task_id:
        return func.HttpResponse(
            "Please provide a 'task_id' parameter in the query string or request body.",
            status_code=400
        )

    # Build the task function to run in background
    def run_task():
        try:
            if task_id == '1':
                os.environ["IDP_VERBOSE"] = "1"
                os.environ.setdefault("IDP_LOG_LEVEL", "DEBUG")
                logging.info("Triggering IDP Accuracy script")
                run_idp_accuracy()
            elif task_id == '2':
                logging.info(f"Triggering Tabak Accuracy script with ids_per_message={ids_per_message}, max_messages_per_run={max_messages_per_run}, folder_name={folder_name}")
                run_tabak_accuracy(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run, folder_name=folder_name)
            elif task_id == '3':
                os.environ["HEALTHCARE_ACCURACY_FILE_TYPE"] = "eob"
                logging.info("Triggering Healthcare EOB Accuracy script")
                run_healthcare_eob_accuracy(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run)
            elif task_id == '4':
                os.environ["HEALTHCARE_ACCURACY_FILE_TYPE"] = "superbill"
                logging.info("Triggering Healthcare Superbill Accuracy script")
                run_healthcare_superbill_accuracy(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run)
            elif task_id == '5':
                logging.info(f"Triggering Tabak Fine Tuning data push with ids_per_message={ids_per_message}, max_messages_per_run={max_messages_per_run}, start_date={start_date}, end_date={end_date}, folder_name={folder_name}, bypass_checkpoint={bypass_checkpoint}")
                tabak_fine_tuning_data_push(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run, start_date=start_date, end_date=end_date, folder_name=folder_name, bypass_checkpoint=bypass_checkpoint)
            elif task_id == '6':
                logging.info(f"Triggering EOB Fine Tuning data push with ids_per_message={ids_per_message}, max_messages_per_run={max_messages_per_run}, start_date={start_date}, end_date={end_date}, folder_name={folder_name}, bypass_checkpoint={bypass_checkpoint}")
                eob_fine_tuning_data_push(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run, start_date=start_date, end_date=end_date, folder_name=folder_name, bypass_checkpoint=bypass_checkpoint)
            elif task_id == '7':
                logging.info(f"Triggering Superbill Fine Tuning data push with ids_per_message={ids_per_message}, max_messages_per_run={max_messages_per_run}, start_date={start_date}, end_date={end_date}, folder_name={folder_name}, bypass_checkpoint={bypass_checkpoint}")
                superbill_fine_tuning_data_push(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run, start_date=start_date, end_date=end_date, folder_name=folder_name, bypass_checkpoint=bypass_checkpoint)
            elif task_id == '8':
                os.environ.pop("HEALTHCARE_ACCURACY_FILE_TYPE", None)
                logging.info(f"Triggering IDP Fine Tuning data push with ids_per_message={ids_per_message}, max_messages_per_run={max_messages_per_run}, start_date={start_date}, end_date={end_date}, folder_name={folder_name}, bypass_checkpoint={bypass_checkpoint}")
                run_idp_fine_tuning_data_push(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run, start_date=start_date, end_date=end_date, folder_name=folder_name, bypass_checkpoint=bypass_checkpoint)
            elif task_id == '9':
                logging.info(f"Triggering Audio (CCAI) Fine Tuning data push with ids_per_message={ids_per_message}, max_messages_per_run={max_messages_per_run}, start_date={start_date}, end_date={end_date}, folder_name={folder_name}, bypass_checkpoint={bypass_checkpoint}")
                audio_fine_tuning_data_push(ids_per_message=ids_per_message, max_messages_per_run=max_messages_per_run, start_date=start_date, end_date=end_date, folder_name=folder_name, bypass_checkpoint=bypass_checkpoint)
            logging.info(f"Task {task_id} completed successfully.")
        except Exception as e:
            logging.error(f"Background task {task_id} failed: {e}")

    if task_id not in ('1', '2', '3', '4', '5', '6', '7', '8', '9'):
        return func.HttpResponse(
            "Please provide a valid 'task_id' parameter (from 1 to 9).",
            status_code=400
        )

    # Execute task SYNCHRONOUSLY (no threading - Azure Functions will manage lifespan)
    logging.info(f"Task {task_id} executing synchronously...")
    try:
        run_task()
        logging.info(f"Task {task_id} completed successfully.")
        return func.HttpResponse(
            f"Task {task_id} completed successfully.",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Task {task_id} failed: {e}")
        return func.HttpResponse(
            f"Task {task_id} failed: {e}",
            status_code=500
        )