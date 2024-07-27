# File to store the process ID
PIDFILE=/tmp/feetierscript.pid

# Check if the PID file exists and the process with that PID is currently running
if [ -f "$PIDFILE" ] && kill -0 $(cat "$PIDFILE") 2> /dev/null; then
    :
else
    # Run the python command
    ./venv_orderly/bin/python app/main.py update-user-rate-base-volume >> run.log 2>&1&
    echo $! > "$PIDFILE"
fi

