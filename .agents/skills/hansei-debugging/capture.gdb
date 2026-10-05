set pagination off
set confirm off
set startup-with-shell off
set use-coredump-filter on
set dump-excluded-mappings off
handle SIGSTOP stop nopass
handle SIGABRT nostop noprint pass
python
import gdb, os, signal, threading
capture_pid = None
capture_timer = None

def arm_capture(event):
    global capture_pid, capture_timer
    if capture_pid is None:
        capture_pid = gdb.selected_inferior().pid
        capture_timer = threading.Timer(
            float(os.environ.get('HANSEI_CAPTURE_AFTER', '70')),
            lambda: os.kill(capture_pid, signal.SIGSTOP),
        )
        capture_timer.daemon = True
        capture_timer.start()

gdb.events.new_thread.connect(arm_capture)
try:
    gdb.execute('run')
    if gdb.selected_inferior().pid:
        if os.environ.get("HANSEI_CAPTURE_MODE", "gdb") == "kernel":
            print("HANSEI_CORE_PID=" + str(capture_pid))
            # Detach before aborting: GDB otherwise races the kernel teardown
            # and can fail while reading registers from the exiting child.
            gdb.execute("detach")
            os.kill(capture_pid, signal.SIGABRT)
        else:
            gdb.execute("generate-core-file " + os.environ["HANSEI_CORE"])
            gdb.execute("kill")
    else:
        print('HANSEI_TARGET_EXITED_BEFORE_CAPTURE')
finally:
    if capture_timer is not None:
        capture_timer.cancel()
end
quit
