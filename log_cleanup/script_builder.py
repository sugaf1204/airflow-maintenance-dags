"""Utility helpers for building the log cleanup DAG shell script."""
from __future__ import annotations

from textwrap import dedent


def build_cleanup_bash_script(
    *,
    default_max_log_age_in_days: int,
    enable_delete: bool,
    lock_file_path: str,
) -> str:
    """Return the templated bash script executed by the cleanup workers.

    Parameters
    ----------
    default_max_log_age_in_days
        Fallback value used when the DAG run configuration does not specify
        ``maxLogAgeInDays``.
    enable_delete
        Indicates whether files should actually be removed or if the script
        should only print what *would* be deleted.
    lock_file_path
        Location for the lock file that protects against concurrent runs on the
        same worker.
    """
    enable_delete_flag = "true" if enable_delete else "false"

    script = dedent(
        f"""
        echo "Getting Configurations..."
        BASE_LOG_FOLDER="{{{{params.directory}}}}"
        WORKER_SLEEP_TIME="{{{{params.sleep_time}}}}"

        sleep ${{WORKER_SLEEP_TIME}}s

        MAX_LOG_AGE_IN_DAYS="{{{{dag_run.conf.maxLogAgeInDays}}}}"
        if [ "${{MAX_LOG_AGE_IN_DAYS}}" == "" ]; then
            echo "maxLogAgeInDays conf variable isn't included. Using Default '{default_max_log_age_in_days}'."
            MAX_LOG_AGE_IN_DAYS='{default_max_log_age_in_days}'
        fi
        ENABLE_DELETE="{enable_delete_flag}"
        echo "Finished Getting Configurations"
        echo ""

        echo "Configurations:"
        echo "BASE_LOG_FOLDER:      '${{BASE_LOG_FOLDER}}'"
        echo "MAX_LOG_AGE_IN_DAYS:  '${{MAX_LOG_AGE_IN_DAYS}}'"
        echo "ENABLE_DELETE:        '${{ENABLE_DELETE}}'"

        cleanup() {{
            echo "Executing Find Statement: $1"
            FILES_MARKED_FOR_DELETE=`eval $1`
            echo "Process will be Deleting the following File(s)/Directory(s):"
            echo "${{FILES_MARKED_FOR_DELETE}}"
            echo "Process will be Deleting `echo \"${{FILES_MARKED_FOR_DELETE}}\" | grep -v '^$' | wc -l` File(s)/Directory(s)"
            echo ""
            if [ "${{ENABLE_DELETE}}" == "true" ];
            then
                if [ "${{FILES_MARKED_FOR_DELETE}}" != "" ];
                then
                    echo "Executing Delete Statement: $2"
                    eval $2
                    DELETE_STMT_EXIT_CODE=$?
                    if [ "${{DELETE_STMT_EXIT_CODE}}" != "0" ]; then
                        echo "Delete process failed with exit code '${{DELETE_STMT_EXIT_CODE}}'"

                        echo "Removing lock file..."
                        rm -f "{lock_file_path}"
                        REMOVE_LOCK_FILE_EXIT_CODE=$?
                        if [ "${{REMOVE_LOCK_FILE_EXIT_CODE}}" != "0" ]; then
                            echo "Error removing the lock file. Check file permissions. To re-run the DAG, ensure that the lock file has been deleted ({lock_file_path})."
                            exit ${{REMOVE_LOCK_FILE_EXIT_CODE}}
                        fi
                        exit ${{DELETE_STMT_EXIT_CODE}}
                    fi
                else
                    echo "WARN: No File(s)/Directory(s) to Delete"
                fi
            else
                echo "WARN: You're opted to skip deleting the File(s)/Directory(s)!!!"
            fi
        }}

        if [ ! -f "{lock_file_path}" ]; then

            echo "Lock file not found on this node! Creating it to prevent collisions..."
            touch "{lock_file_path}"
            CREATE_LOCK_FILE_EXIT_CODE=$?
            if [ "${{CREATE_LOCK_FILE_EXIT_CODE}}" != "0" ]; then
                echo "Error creating the lock file. Check if the airflow user can create files under tmp directory. Exiting..."
                exit ${{CREATE_LOCK_FILE_EXIT_CODE}}
            fi

            echo ""
            echo "Running Cleanup Process..."

            FIND_STATEMENT="find ${{BASE_LOG_FOLDER}}/*/* -type f -mtime +${{MAX_LOG_AGE_IN_DAYS}}"
            DELETE_STMT="${{FIND_STATEMENT}} -exec rm -f {{}} \\;"

            cleanup "${{FIND_STATEMENT}}" "${{DELETE_STMT}}"
            CLEANUP_EXIT_CODE=$?

            FIND_STATEMENT="find ${{BASE_LOG_FOLDER}}/*/* -type d -empty"
            DELETE_STMT="${{FIND_STATEMENT}} -prune -exec rm -rf {{}} \\;"

            cleanup "${{FIND_STATEMENT}}" "${{DELETE_STMT}}"
            CLEANUP_EXIT_CODE=$?

            FIND_STATEMENT="find ${{BASE_LOG_FOLDER}}/* -type d -empty"
            DELETE_STMT="${{FIND_STATEMENT}} -prune -exec rm -rf {{}} \\;"

            cleanup "${{FIND_STATEMENT}}" "${{DELETE_STMT}}"
            CLEANUP_EXIT_CODE=$?

            echo "Finished Running Cleanup Process"

            echo "Deleting lock file..."
            rm -f "{lock_file_path}"
            REMOVE_LOCK_FILE_EXIT_CODE=$?
            if [ "${{REMOVE_LOCK_FILE_EXIT_CODE}}" != "0" ]; then
                echo "Error removing the lock file. Check file permissions. To re-run the DAG, ensure that the lock file has been deleted ({lock_file_path})."
                exit ${{REMOVE_LOCK_FILE_EXIT_CODE}}
            fi

        else
            echo "Another task is already deleting logs on this worker node. Skipping it!"
            echo "If you believe you're receiving this message in error, kindly check if {lock_file_path} exists and delete it."
            exit 0
        fi
        """
    ).strip()

    return script
