import csv
import queue
import threading
import tkinter as tk
from collections import deque
from tkinter import filedialog
from tkinter import ttk
import time
import statistics

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import serial
from serial.tools import list_ports


class CdcGuiApp:
    RX_RATE_HZ = 50.0
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("PID Tuner")
        self.root.geometry("900x700")
        self.root.minsize(800, 600)

        self.serial_port = None
        self.reader_thread = None
        self.stop_event = threading.Event()
        self.rx_queue = queue.Queue()
        self.rx_buffer = ""
        self.last_rx_line = None
        self.last_rx_pair = None

        self.plot_times = deque(maxlen=300)
        self.plot_target = deque(maxlen=300)
        self.plot_actual = deque(maxlen=300)
        self.actual_history = deque()
        self.plot_index = 0
        self.last_device_time = None
        self.last_target = None
        self.pending_step_start = False
        self.step_prev_target = None
        self.rx_rate_hz = None
        self.rx_rate_var = tk.StringVar(value="RX rate: -- Hz")
        self.response_plot_window = None
        self.response_plot_active = False
        self.response_plot_end_time = None
        self.response_plot_t0 = None
        self.response_plot_start_elapsed = None
        self.response_plot_duration = None
        self.response_plot_after_id = None
        self.response_plot_paused = False
        self.response_plot_save_path = None
        self.response_plot_annotations = []
        self.response_plot_markers = []
        self.response_point_dragging = False
        self.response_point_drag_index = None
        self.response_cursor_a = None
        self.response_cursor_b = None
        self.response_cursor_line_a = None
        self.response_cursor_line_b = None
        self.response_cursor_active = None
        self.response_cursor_dragging = None
        self.response_metrics_var = tk.StringVar(value="Cursors: --")
        self.response_seq = 0
        self.response_plot_times = deque()
        self.response_plot_target = deque()
        self.response_plot_actual = deque()
        self.response_plot_canvas = None
        self.response_plot_axes = None
        self.response_plot_target_line = None
        self.response_plot_actual_line = None

        self.recording = False
        self.csv_file = None
        self.csv_writer = None

        self.step_active = False
        self.step_target = None
        self.step_start_time = None
        self.settle_start_time = None
        self.settled_time = None
        self.overshoot_max = 0.0

        self._build_ui()
        self._poll_rx_queue()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root)
        container.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(container, highlightthickness=0)
        h_scroll = ttk.Scrollbar(container, orient=tk.HORIZONTAL, command=canvas.xview)
        v_scroll = ttk.Scrollbar(container, orient=tk.VERTICAL, command=canvas.yview)
        canvas.configure(xscrollcommand=h_scroll.set, yscrollcommand=v_scroll.set)

        canvas.grid(row=0, column=0, sticky="nsew")
        v_scroll.grid(row=0, column=1, sticky="ns")
        h_scroll.grid(row=1, column=0, sticky="ew")

        container.rowconfigure(0, weight=1)
        container.columnconfigure(0, weight=1)

        main = ttk.Frame(canvas, padding=12)
        canvas_window = canvas.create_window((0, 0), window=main, anchor="nw")

        def _on_frame_configure(event):
            main.update_idletasks()
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _on_canvas_configure(event):
            main.update_idletasks()
            desired_height = max(event.height, main.winfo_reqheight())
            canvas.itemconfigure(canvas_window, width=event.width, height=desired_height)

        main.bind("<Configure>", _on_frame_configure)
        canvas.bind("<Configure>", _on_canvas_configure)

        notebook = ttk.Notebook(main)
        notebook.pack(fill=tk.X)

        connection_tab = ttk.Frame(notebook, padding=10)
        controller_tab = ttk.Frame(notebook, padding=10)
        response_tab = ttk.Frame(notebook, padding=10)

        notebook.add(connection_tab, text="Connection")
        notebook.add(controller_tab, text="Controller Settings")
        notebook.add(response_tab, text="Response Generator")

        connection_frame = ttk.LabelFrame(connection_tab, text="Connection", padding=10)
        connection_frame.pack(fill=tk.X)

        ttk.Label(connection_frame, text="COM Port:").grid(row=0, column=0, sticky=tk.W)
        self.port_var = tk.StringVar()
        self.port_combo = ttk.Combobox(
            connection_frame, textvariable=self.port_var, width=16, state="readonly"
        )
        self.port_combo.grid(row=0, column=1, padx=6, sticky=tk.W)

        ttk.Label(connection_frame, text="VID:").grid(row=0, column=2, sticky=tk.W)
        self.vid_var = tk.StringVar(value="0483")
        ttk.Entry(connection_frame, textvariable=self.vid_var, width=6).grid(
            row=0, column=3, padx=4, sticky=tk.W
        )

        ttk.Label(connection_frame, text="PID:").grid(row=0, column=4, sticky=tk.W)
        self.pid_var = tk.StringVar(value="5740")
        ttk.Entry(connection_frame, textvariable=self.pid_var, width=6).grid(
            row=0, column=5, padx=4, sticky=tk.W
        )

        ttk.Label(connection_frame, text="Baud:").grid(row=0, column=6, sticky=tk.W)
        self.baud_var = tk.StringVar(value="115200")
        ttk.Entry(connection_frame, textvariable=self.baud_var, width=10).grid(
            row=0, column=7, padx=6, sticky=tk.W
        )

        ttk.Button(
            connection_frame, text="Refresh", command=self._refresh_ports
        ).grid(row=0, column=8, padx=4)

        ttk.Button(
            connection_frame, text="Auto Detect", command=self._auto_detect_port
        ).grid(row=0, column=9, padx=4)

        self.connect_button = ttk.Button(
            connection_frame, text="Connect", command=self._toggle_connection
        )
        self.connect_button.grid(row=0, column=10, padx=6)

        formula_frame = ttk.LabelFrame(controller_tab, text="Compensator Formula", padding=10)
        formula_frame.pack(fill=tk.X)
        ttk.Label(
            formula_frame,
            text="u[k] = u[k-1] + Kp*(e[k]-e[k-1]) + Ki*Ts*e[k] + Kd*(e[k]-2e[k-1]+e[k-2])/Ts",
            wraplength=600,
        ).pack(anchor=tk.W)

        controller_tabs = ttk.Notebook(controller_tab)
        controller_tabs.pack(fill=tk.X, pady=(10, 0))

        settings_tab = ttk.Frame(controller_tabs, padding=10)
        current_pid_tab = ttk.Frame(controller_tabs, padding=10)
        tuning_pid_tab = ttk.Frame(controller_tabs, padding=10)

        controller_tabs.add(settings_tab, text="Controller Settings")
        controller_tabs.add(current_pid_tab, text="Current PID")
        controller_tabs.add(tuning_pid_tab, text="Automated Tuning ")

        control_frame = ttk.LabelFrame(settings_tab, text="Controller Settings", padding=10)
        control_frame.pack(fill=tk.X)

        ttk.Label(control_frame, text="Sample Time (ms):").grid(
            row=0, column=0, sticky=tk.W
        )
        self.sample_time_var = tk.StringVar(value="2")
        self.sample_time_entry = ttk.Entry(
            control_frame, textvariable=self.sample_time_var, width=8, state="disabled"
        )
        self.sample_time_entry.grid(row=0, column=1, padx=(12, 4), sticky=tk.W)
        ttk.Label(control_frame, text="Proportional (P):").grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
        self.p_var = tk.StringVar(value="1.2")
        ttk.Entry(control_frame, textvariable=self.p_var, width=8).grid(
            row=1, column=1, padx=(12, 4), sticky=tk.W, pady=(6, 0)
        )
        ttk.Label(control_frame, text="Integral (I):").grid(row=2, column=0, sticky=tk.W, pady=(6, 0))
        self.i_var = tk.StringVar(value="0.5")
        ttk.Entry(control_frame, textvariable=self.i_var, width=8).grid(
            row=2, column=1, padx=(12, 4), sticky=tk.W, pady=(6, 0)
        )
        ttk.Label(control_frame, text="Derivative (D):").grid(row=3, column=0, sticky=tk.W, pady=(6, 0))
        self.d_var = tk.StringVar(value="0.01")
        ttk.Entry(control_frame, textvariable=self.d_var, width=8).grid(
            row=3, column=1, padx=(12, 4), sticky=tk.W, pady=(6, 0)
        )
        ttk.Button(control_frame, text="Update Controller", command=self._update_controller).grid(
            row=4, column=0, padx=6, pady=(8, 0), sticky=tk.W
        )

        current_pid_frame = ttk.LabelFrame(current_pid_tab, text="Current PID", padding=10)
        current_pid_frame.pack(fill=tk.X)
        self.current_p_var = tk.StringVar(value="--")
        self.current_i_var = tk.StringVar(value="--")
        self.current_d_var = tk.StringVar(value="--")
        ttk.Label(current_pid_frame, text="Proportional (P):").grid(row=0, column=0, sticky=tk.W)
        ttk.Label(current_pid_frame, textvariable=self.current_p_var).grid(
            row=0, column=1, padx=(12, 0), sticky=tk.W
        )
        ttk.Label(current_pid_frame, text="Integral (I):").grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
        ttk.Label(current_pid_frame, textvariable=self.current_i_var).grid(
            row=1, column=1, padx=(12, 0), sticky=tk.W, pady=(6, 0)
        )
        ttk.Label(current_pid_frame, text="Derivative (D):").grid(row=2, column=0, sticky=tk.W, pady=(6, 0))
        ttk.Label(current_pid_frame, textvariable=self.current_d_var).grid(
            row=2, column=1, padx=(12, 0), sticky=tk.W, pady=(6, 0)
        )
        ttk.Button(current_pid_frame, text="Get Controller Parameters ", command=self._send_get_pid).grid(
            row=3, column=0, padx=6, pady=(8, 0), sticky=tk.W
        )

        tuning_pid_frame = ttk.LabelFrame(tuning_pid_tab, text="Automated Tuning", padding=10)
        tuning_pid_frame.pack(fill=tk.X)
        ttk.Label(tuning_pid_frame, text="Method:").grid(row=0, column=0, sticky=tk.W)
        self.tuning_method_var = tk.StringVar(value="Nicholas Ziegler")
        tuning_combo = ttk.Combobox(
            tuning_pid_frame,
            textvariable=self.tuning_method_var,
            state="readonly",
            values=["Nicholas Ziegler", "AI-Tuner", "Relay (Astrom-Hagglund)"],
            width=18,
        )
        tuning_combo.grid(row=0, column=1, padx=6, sticky=tk.W)
        tuning_combo.bind("<<ComboboxSelected>>", self._update_tune_fields)
        ttk.Button(tuning_pid_frame, text="Tune", command=self._send_tune).grid(
            row=0, column=2, padx=6
        )
        ttk.Button(tuning_pid_frame, text="Stop Tune", command=self._send_tune_stop).grid(
            row=0, column=3, padx=6
        )

        self.tune_status_var = tk.StringVar(value="Tune: --")
        ttk.Label(tuning_pid_frame, textvariable=self.tune_status_var).grid(
            row=2, column=0, columnspan=3, sticky=tk.W, pady=(6, 0)
        )

        relay_frame = ttk.LabelFrame(tuning_pid_frame, text="Relay Tuning", padding=8)
        relay_frame.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(8, 0))
        relay_frame.columnconfigure(0, weight=1)

        self.relay_sp_var = tk.StringVar(value="0.0")
        self.relay_fs_var = tk.StringVar(value="100")
        self.relay_d_var = tk.StringVar(value="7")
        self.relay_h_var = tk.StringVar(value="1")
        self.relay_cycles_var = tk.StringVar(value="6")
        self.relay_pvmin_var = tk.StringVar(value="0")
        self.relay_pvmax_var = tk.StringVar(value="360")

        ttk.Label(relay_frame, text="Setpoint:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(relay_frame, textvariable=self.relay_sp_var, width=10).grid(
            row=0, column=1, padx=6, sticky=tk.W
        )
        ttk.Label(relay_frame, text="FS (%):").grid(row=0, column=2, sticky=tk.W)
        ttk.Entry(relay_frame, textvariable=self.relay_fs_var, width=8).grid(
            row=0, column=3, padx=6, sticky=tk.W
        )
        ttk.Label(relay_frame, text="d (%):").grid(row=0, column=4, sticky=tk.W)
        ttk.Entry(relay_frame, textvariable=self.relay_d_var, width=8).grid(
            row=0, column=5, padx=6, sticky=tk.W
        )

        ttk.Label(relay_frame, text="h (% PV):").grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
        ttk.Entry(relay_frame, textvariable=self.relay_h_var, width=10).grid(
            row=1, column=1, padx=6, sticky=tk.W, pady=(6, 0)
        )
        ttk.Label(relay_frame, text="Cycles:").grid(row=1, column=2, sticky=tk.W, pady=(6, 0))
        ttk.Entry(relay_frame, textvariable=self.relay_cycles_var, width=8).grid(
            row=1, column=3, padx=6, sticky=tk.W, pady=(6, 0)
        )
        ttk.Label(relay_frame, text="PV Min:").grid(row=1, column=4, sticky=tk.W, pady=(6, 0))
        ttk.Entry(relay_frame, textvariable=self.relay_pvmin_var, width=8).grid(
            row=1, column=5, padx=6, sticky=tk.W, pady=(6, 0)
        )
        ttk.Label(relay_frame, text="PV Max:").grid(row=1, column=6, sticky=tk.W, pady=(6, 0))
        ttk.Entry(relay_frame, textvariable=self.relay_pvmax_var, width=8).grid(
            row=1, column=7, padx=6, sticky=tk.W, pady=(6, 0)
        )

        self.relay_frame = relay_frame
        self._update_tune_fields()

        response_frame = ttk.LabelFrame(response_tab, text="Response Generator", padding=10)
        response_frame.pack(fill=tk.X)

        ttk.Label(response_frame, text="Type:").grid(row=0, column=0, sticky=tk.W)
        self.response_type_var = tk.StringVar(value="Setpoint")
        self.response_type_combo = ttk.Combobox(
            response_frame,
            textvariable=self.response_type_var,
            state="readonly",
            width=12,
            values=["Setpoint", "Step", "Ramp", "Accel", "Sine"],
        )
        self.response_type_combo.grid(row=0, column=1, padx=6, sticky=tk.W)
        self.response_type_combo.bind("<<ComboboxSelected>>", self._update_response_fields)

        self.setpoint_var = tk.StringVar(value="0.0")
        self.step_var = tk.StringVar(value="0.0")
        self.ramp_var = tk.StringVar(value="0.0")
        self.accel_var = tk.StringVar(value="0.0")
        self.sine_amp_var = tk.StringVar(value="0.0")
        self.sine_freq_var = tk.StringVar(value="1.0")
        self.sine_offset_var = tk.StringVar(value="0.0")
        self.response_time_var = tk.StringVar(value="2.0")
        self.use_time_var = tk.BooleanVar(value=True)

        self.setpoint_label = ttk.Label(response_frame, text="Setpoint:")
        self.setpoint_entry = ttk.Entry(response_frame, textvariable=self.setpoint_var, width=10)

        self.step_label = ttk.Label(response_frame, text="Amplitude:")
        self.step_entry = ttk.Entry(response_frame, textvariable=self.step_var, width=10)

        self.ramp_label = ttk.Label(response_frame, text="Slope:")
        self.ramp_entry = ttk.Entry(response_frame, textvariable=self.ramp_var, width=10)

        self.accel_label = ttk.Label(response_frame, text="Value:")
        self.accel_entry = ttk.Entry(response_frame, textvariable=self.accel_var, width=10)

        self.sine_amp_label = ttk.Label(response_frame, text="Amplitude:")
        self.sine_amp_entry = ttk.Entry(response_frame, textvariable=self.sine_amp_var, width=10)
        self.sine_freq_label = ttk.Label(response_frame, text="Frequency:")
        self.sine_freq_hint = ttk.Label(response_frame, text="Range 0-100 Hz")
        self.sine_freq_entry = ttk.Entry(response_frame, textvariable=self.sine_freq_var, width=10)
        self.sine_offset_label = ttk.Label(response_frame, text="Offset:")
        self.sine_offset_entry = ttk.Entry(response_frame, textvariable=self.sine_offset_var, width=10)

        self.time_label = ttk.Label(response_frame, text="Time (s):")
        self.time_entry = ttk.Entry(response_frame, textvariable=self.response_time_var, width=10)
        self.time_check = ttk.Checkbutton(
            response_frame,
            text="Use Time",
            variable=self.use_time_var,
            command=self._toggle_time_entry,
        )

        self.response_send_button = ttk.Button(
            response_frame, text="Send", command=self._send_response
        )
        self.response_send_button.grid(row=0, column=6, padx=6)
        self.response_plot_button = ttk.Button(
            response_frame, text="Open Plot", command=self._open_response_plot_manual
        )
        self.response_plot_button.grid(row=0, column=7, padx=6)
        self.estop_button = ttk.Button(
            response_frame, text="Force Stop", command=self._send_estop
        )
        self.estop_button.grid(row=0, column=8, padx=6)

        self._update_response_fields()

        self.settling_time_var = tk.StringVar(value="--")
        self.overshoot_var = tk.StringVar(value="--")
        self.sse_var = tk.StringVar(value="--")
        self.sse_percent_var = tk.BooleanVar(value=False)

        toggle_frame = ttk.Frame(main)
        toggle_frame.pack(fill=tk.X, pady=(10, 0))
        self.show_plot_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            toggle_frame,
            text="Show Graph",
            variable=self.show_plot_var,
            command=self._toggle_plot,
        ).pack(side=tk.LEFT)
        self.show_log_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            toggle_frame,
            text="Show Log",
            variable=self.show_log_var,
            command=self._toggle_log,
        ).pack(side=tk.LEFT, padx=10)

        self.paned = ttk.Panedwindow(main, orient=tk.HORIZONTAL)
        self.paned.pack(fill=tk.BOTH, expand=True, pady=(6, 0))

        self.plot_frame = ttk.LabelFrame(self.paned, text="Target vs Actual", padding=10)

        self.figure = Figure(figsize=(5, 2.5), dpi=100)
        self.axes = self.figure.add_subplot(111)
        self.axes.set_xlabel("Time (s)")
        self.axes.set_ylabel("Value")
        self.axes.grid(True, alpha=0.3)
        self.target_line, = self.axes.plot([], [], label="Target")
        self.actual_line, = self.axes.plot([], [], label="Actual")
        self.axes.legend(loc="upper right")

        self.canvas = FigureCanvasTkAgg(self.figure, master=self.plot_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        capture_frame = ttk.Frame(self.plot_frame)
        capture_frame.pack(fill=tk.X, pady=(8, 0))

        self.record_button = ttk.Button(
            capture_frame, text="Start Recording", command=self._start_recording
        )
        self.record_button.pack(side=tk.LEFT)

        self.stop_button = ttk.Button(
            capture_frame, text="Stop Recording", command=self._stop_recording, state="disabled"
        )
        self.stop_button.pack(side=tk.LEFT, padx=6)

        self.rx_rate_label = ttk.Label(capture_frame, textvariable=self.rx_rate_var)
        self.rx_rate_label.pack(side=tk.RIGHT)

        self.log_frame = ttk.LabelFrame(self.paned, text="RX/TX Log", padding=10)

        self.log_text = tk.Text(self.log_frame, height=12, wrap="word", state="disabled")
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(self.log_frame, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.configure(yscrollcommand=scrollbar.set)

        self.paned.add(self.log_frame, weight=2)
        self.paned.add(self.plot_frame, weight=3)

        self._refresh_ports()

    def _log(self, message: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def _toggle_connection(self) -> None:
        if self.serial_port:
            self._disconnect()
        else:
            self._connect()

    def _connect(self) -> None:
        port = self.port_var.get().strip()
        if not port:
            self._log("ERR: COM port is empty.")
            return

        try:
            baud = int(self.baud_var.get())
        except ValueError:
            self._log("ERR: baud must be a number.")
            return

        try:
            self.serial_port = serial.Serial(port, baudrate=baud, timeout=0.1)
        except serial.SerialException as exc:
            self._log(f"ERR: failed to open {port}: {exc}")
            self.serial_port = None
            return

        self.stop_event.clear()
        self.reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self.reader_thread.start()

        self.connect_button.configure(text="Disconnect")
        self._log(f"Connected to {port} @ {baud}")

    def _disconnect(self) -> None:
        self.stop_event.set()
        if self.reader_thread and self.reader_thread.is_alive():
            self.reader_thread.join(timeout=1.0)

        if self.serial_port:
            self.serial_port.close()
            self.serial_port = None

        self.connect_button.configure(text="Connect")
        self._log("Disconnected.")

    def _refresh_ports(self) -> None:
        ports = [info.device for info in list_ports.comports()]
        self.port_combo["values"] = ports
        if ports and not self.port_var.get():
            self.port_var.set(ports[0])

    def _auto_detect_port(self) -> None:
        target_vid = self._parse_hex(self.vid_var.get())
        target_pid = self._parse_hex(self.pid_var.get())

        candidates = list_ports.comports()
        best = None

        for info in candidates:
            if target_vid is not None and info.vid != target_vid:
                continue
            if target_pid is not None and info.pid != target_pid:
                continue
            best = info.device
            break

        if not best:
            for info in candidates:
                text = " ".join(
                    [
                        str(info.manufacturer or ""),
                        str(info.product or ""),
                        str(info.description or ""),
                    ]
                ).upper()
                if "STM" in text or "STMicroelectronics".upper() in text:
                    best = info.device
                    break

        if not best and candidates:
            best = candidates[0].device

        if best:
            self.port_var.set(best)
            self._log(f"Auto-detected port: {best}")
        else:
            self._log("ERR: no COM ports found.")

    @staticmethod
    def _parse_hex(value: str) -> int | None:
        text = value.strip().lower()
        if not text:
            return None
        if text.startswith("0x"):
            text = text[2:]
        try:
            return int(text, 16)
        except ValueError:
            return None

    def _send_pid(self) -> bool:
        if not self.serial_port or not self.serial_port.is_open:
            self._log("ERR: not connected.")
            return False

        try:
            p_val = float(self.p_var.get())
            i_val = float(self.i_var.get())
            d_val = float(self.d_var.get())
        except ValueError:
            self._log("ERR: PID values must be numbers.")
            return False

        payload = f"P={p_val},I={i_val},D={d_val}\n"
        self.serial_port.write(payload.encode("utf-8"))
        self._log(f"TX: {payload.strip()}")
        if response_type == "Step":
            self.pending_step_start = True
            self.last_target = None
        else:
            self.step_active = False
            self.step_target = None
            self.step_start_time = None
        if duration is not None:
            self._set_response_plot_duration(duration)
        return True

    def _send_sample_time(self) -> bool:
        sample_time_ms = self._validate_sample_time()
        if sample_time_ms is None:
            return False

        if not self.serial_port or not self.serial_port.is_open:
            self._log("ERR: not connected.")
            return False

        payload = f"TS={sample_time_ms}\n"
        self.serial_port.write(payload.encode("utf-8"))
        self._log(f"TX: {payload.strip()}")
        return True

    def _update_controller(self) -> None:
        if self._validate_sample_time() is None:
            return
        if not self._send_pid():
            return
        self._send_sample_time()

    def _send_tune(self) -> None:
        if not self.serial_port or not self.serial_port.is_open:
            self._log("ERR: not connected.")
            return
        method = self.tuning_method_var.get().strip()
        if method.startswith("Relay"):
            try:
                sp = float(self.relay_sp_var.get())
                fs = float(self.relay_fs_var.get())
                d = float(self.relay_d_var.get())
                h = float(self.relay_h_var.get())
                cycles = int(float(self.relay_cycles_var.get()))
                pv_min = float(self.relay_pvmin_var.get())
                pv_max = float(self.relay_pvmax_var.get())
            except ValueError:
                self._log("ERR: relay tuning parameters must be numeric.")
                return
            payload = (
                f"TUNE=RELAY,SP={sp},FS={fs},D={d},H={h},CYC={cycles},"
                f"PV_MIN={pv_min},PV_MAX={pv_max}\n"
            )
        else:
            payload = f"TUNE={method}\n"
        self.serial_port.write(payload.encode("utf-8"))
        self._log(f"TX: {payload.strip()}")

    def _send_tune_stop(self) -> None:
        if not self.serial_port or not self.serial_port.is_open:
            self._log("ERR: not connected.")
            return
        payload = "TUNE=STOP\n"
        self.serial_port.write(payload.encode("utf-8"))
        self._log("TX: TUNE=STOP")

    def _update_tune_fields(self, event=None) -> None:
        method = self.tuning_method_var.get().strip()
        if method.startswith("Relay"):
            self.relay_frame.grid()
        else:
            self.relay_frame.grid_remove()

    def _send_get_pid(self) -> None:
        if not self.serial_port or not self.serial_port.is_open:
            self._log("ERR: not connected.")
            return
        payload = "GETPID\n"
        self.serial_port.write(payload.encode("utf-8"))
        self._log("TX: GETPID")

    def _send_estop(self) -> None:
        if not self.serial_port or not self.serial_port.is_open:
            self._log("ERR: not connected.")
            return
        payload = "ESTOP\n"
        self.serial_port.write(payload.encode("utf-8"))
        self._log("TX: ESTOP")

    def _validate_sample_time(self) -> float | None:
        try:
            sample_time_ms = float(self.sample_time_var.get())
        except ValueError:
            self._log("ERR: sample time must be a number.")
            return None

        if sample_time_ms < 2 or sample_time_ms > 500:
            self._log("ERR: sample time must be 2-500 ms.")
            return None

        return sample_time_ms

    def _update_response_fields(self, event=None) -> None:
        widgets = [
            self.setpoint_label,
            self.setpoint_entry,
            self.step_label,
            self.step_entry,
            self.ramp_label,
            self.ramp_entry,
            self.accel_label,
            self.accel_entry,
            self.sine_amp_label,
            self.sine_amp_entry,
            self.sine_freq_label,
            self.sine_freq_entry,
            self.sine_offset_label,
            self.sine_offset_entry,
            self.time_label,
            self.time_entry,
            self.time_check,
        ]
        for widget in widgets:
            widget.grid_remove()

        response_type = self.response_type_var.get()
        if response_type == "Setpoint":
            self.setpoint_label.grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
            self.setpoint_entry.grid(row=1, column=1, padx=6, sticky=tk.W, pady=(6, 0))
            self.time_label.grid(row=1, column=2, sticky=tk.W, pady=(6, 0))
            self.time_entry.grid(row=1, column=3, padx=6, sticky=tk.W, pady=(6, 0))
            self.time_check.grid(row=1, column=4, padx=6, sticky=tk.W, pady=(6, 0))
        elif response_type == "Step":
            self.step_label.grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
            self.step_entry.grid(row=1, column=1, padx=6, sticky=tk.W, pady=(6, 0))
            self.time_label.grid(row=1, column=2, sticky=tk.W, pady=(6, 0))
            self.time_entry.grid(row=1, column=3, padx=6, sticky=tk.W, pady=(6, 0))
            self.time_check.grid(row=1, column=4, padx=6, sticky=tk.W, pady=(6, 0))
        elif response_type == "Ramp":
            self.ramp_label.grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
            self.ramp_entry.grid(row=1, column=1, padx=6, sticky=tk.W, pady=(6, 0))
            self.time_label.grid(row=1, column=2, sticky=tk.W, pady=(6, 0))
            self.time_entry.grid(row=1, column=3, padx=6, sticky=tk.W, pady=(6, 0))
            self.time_check.grid(row=1, column=4, padx=6, sticky=tk.W, pady=(6, 0))
        elif response_type == "Accel":
            self.accel_label.grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
            self.accel_entry.grid(row=1, column=1, padx=6, sticky=tk.W, pady=(6, 0))
            self.time_label.grid(row=1, column=2, sticky=tk.W, pady=(6, 0))
            self.time_entry.grid(row=1, column=3, padx=6, sticky=tk.W, pady=(6, 0))
            self.time_check.grid(row=1, column=4, padx=6, sticky=tk.W, pady=(6, 0))
        elif response_type == "Sine":
            self.sine_amp_label.grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
            self.sine_amp_entry.grid(row=1, column=1, padx=6, sticky=tk.W, pady=(6, 0))
            self.sine_freq_label.grid(row=1, column=2, sticky=tk.W, pady=(6, 0))
            self.sine_freq_entry.grid(row=1, column=3, padx=6, sticky=tk.W, pady=(6, 0))
            self.sine_freq_hint.grid(row=2, column=2, columnspan=2, sticky=tk.W)
            self.sine_offset_label.grid(row=1, column=4, sticky=tk.W, pady=(6, 0))
            self.sine_offset_entry.grid(row=1, column=5, padx=6, sticky=tk.W, pady=(6, 0))
            self.time_label.grid(row=1, column=6, sticky=tk.W, pady=(6, 0))
            self.time_entry.grid(row=1, column=7, padx=6, sticky=tk.W, pady=(6, 0))
            self.time_check.grid(row=1, column=8, padx=6, sticky=tk.W, pady=(6, 0))

    def _send_response(self) -> None:
        # Open the response plot immediately on button press.
        self.root.after(0, lambda: self._open_response_plot(None))
        if not self.serial_port or not self.serial_port.is_open:
            self._log("ERR: not connected.")
            return
        response_type = self.response_type_var.get()
        try:
            time_text = self.response_time_var.get().strip()
            duration = None
            if self.use_time_var.get():
                duration = float(time_text)
                if duration < 2:
                    self._log("ERR: response time must be >= 2 seconds.")
                    return
            self.response_seq += 1
            seq = self.response_seq

            if response_type == "Setpoint":
                setpoint = float(self.setpoint_var.get())
                if duration is None:
                    payload = f"SETPOINT={setpoint},SEQ={seq}\n"
                else:
                    payload = f"SETPOINT={setpoint},{duration},SEQ={seq}\n"
            elif response_type == "Step":
                if duration is None:
                    self._log("ERR: response time is required.")
                    return
                amplitude = float(self.step_var.get())
                payload = f"STEP={amplitude},{duration},SEQ={seq}\n"
            elif response_type == "Ramp":
                if duration is None:
                    self._log("ERR: response time is required.")
                    return
                slope = float(self.ramp_var.get())
                payload = f"RAMP={slope},{duration},SEQ={seq}\n"
            elif response_type == "Accel":
                if duration is None:
                    self._log("ERR: response time is required.")
                    return
                accel = float(self.accel_var.get())
                payload = f"ACCEL={accel},{duration},SEQ={seq}\n"
            elif response_type == "Sine":
                if duration is None:
                    self._log("ERR: response time is required.")
                    return
                amp = float(self.sine_amp_var.get())
                freq = float(self.sine_freq_var.get())
                offset = float(self.sine_offset_var.get())
                max_freq = 100.0
                if freq < 0:
                    self._log("ERR: sine frequency must be >= 0 Hz.")
                    return
                if freq > max_freq:
                    self._log(f"ERR: sine frequency must be <= {max_freq:g} Hz.")
                    return
                payload = f"SINE={amp},{freq},{offset},{duration},SEQ={seq}\n"
                if freq >= 0.5 * self.RX_RATE_HZ:
                    self._log(
                        f"WARN: sine freq {freq:g} Hz is near/above Nyquist "
                        f"({self.RX_RATE_HZ/2:.1f} Hz). Expect aliasing/distortion."
                    )
            else:
                self._log("ERR: unknown response type.")
                return
        except ValueError:
            self._log("ERR: response parameters must be numbers.")
            return

        self.serial_port.write(payload.encode("utf-8"))
        self._log(f"TX: {payload.strip()}")

    def _toggle_time_entry(self) -> None:
        state = "normal" if self.use_time_var.get() else "disabled"
        self.time_entry.configure(state=state)

    def _reader_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                waiting = self.serial_port.in_waiting if self.serial_port else 0
                if waiting:
                    data = self.serial_port.read(waiting)
                else:
                    data = self.serial_port.read(1)

                if data:
                    try:
                        text = data.decode("utf-8", errors="replace")
                    except UnicodeDecodeError:
                        text = ""
                    if text:
                        self.rx_queue.put(text)
            except serial.SerialException as exc:
                self.rx_queue.put(f"!ERR: serial read failed: {exc}\n")
                break

    def _poll_rx_queue(self) -> None:
        while True:
            try:
                message = self.rx_queue.get_nowait()
            except queue.Empty:
                break
            else:
                self._handle_rx_text(message)

        self._update_plot()
        self.root.after(100, self._poll_rx_queue)

    def _handle_rx_text(self, text: str) -> None:
        self.rx_buffer += text
        if "!ERR:" in text:
            self._log(text.strip())

        while "\n" in self.rx_buffer:
            line, self.rx_buffer = self.rx_buffer.split("\n", 1)
            line = line.strip()
            if not line:
                continue
            if self._should_log_rx(line):
                self._log(f"RX: {line}")
            if self._parse_pid_status(line):
                continue
            if self._parse_tune_status(line):
                continue
            if self._parse_batch(line):
                continue
            self._parse_target_actual(line)

    def _parse_target_actual(self, line: str) -> None:
        if "," not in line:
            return
        left, right = line.split(",", 1)
        try:
            target = float(left.strip())
            actual = float(right.strip())
        except ValueError:
            return
        self._append_sample(target, actual)

    def _should_log_rx(self, line: str) -> bool:
        if line.startswith("B,"):
            parts = line.split(",")
            if len(parts) >= 6:
                try:
                    target = float(parts[4].strip())
                    actual = float(parts[5].strip())
                except ValueError:
                    target = actual = None
                pair = (target, actual)
                if pair == self.last_rx_pair:
                    return False
                self.last_rx_pair = pair
        else:
            if "," in line:
                left, right = line.split(",", 1)
                try:
                    target = float(left.strip())
                    actual = float(right.strip())
                except ValueError:
                    target = actual = None
                pair = (target, actual)
                if pair == self.last_rx_pair:
                    return False
                self.last_rx_pair = pair
        if line == self.last_rx_line:
            return False
        self.last_rx_line = line
        return True

    def _parse_batch(self, line: str) -> bool:
        if not line.startswith("B,"):
            return False
        parts = line.split(",")
        if len(parts) < 5:
            return True
        try:
            t0_ms = float(parts[1])
            dt_ms = float(parts[2])
            count = int(parts[3])
        except ValueError:
            return True
        if count <= 0:
            return True
        expected = 4 + (count * 2)
        if len(parts) < expected:
            return True
        index = 4
        for i in range(count):
            try:
                target = float(parts[index])
                actual = float(parts[index + 1])
            except ValueError:
                break
            t_ms = t0_ms + (i * dt_ms)
            self._append_sample(target, actual, device_time_ms=t_ms)
            index += 2
        return True

    def _append_sample(self, target: float, actual: float, device_time_ms: float | None = None) -> None:
        now_wall = time.time()
        if device_time_ms is None:
            elapsed = self.plot_index / self.RX_RATE_HZ
            self.plot_index += 1
        else:
            elapsed = device_time_ms / 1000.0
            if self.last_device_time is not None and elapsed < self.last_device_time:
                # Device time reset or wrapped; reset plot index fallback.
                self.plot_index = 0
            self.last_device_time = elapsed
        if self.response_type_var.get() == "Step":
            if self.last_target is None:
                self.last_target = target
            else:
                if abs(target - self.last_target) > 1e-6:
                    # Restart metrics on any target change (new step).
                    self._start_step_metrics(
                        target, start_time=elapsed, prev_target=self.last_target
                    )
                    self.pending_step_start = False
                self.last_target = target
        if self.step_active and self.step_start_time is None:
            # Align step timing to the data timebase.
            self.step_start_time = elapsed
        self.plot_times.append(elapsed)
        self.plot_target.append(target)
        self.plot_actual.append(actual)
        self.actual_history.append((elapsed, actual))
        self._trim_history(elapsed)
        self._update_step_metrics(actual, elapsed)
        if self.recording and self.csv_writer:
            self.csv_writer.writerow([now_wall, target, actual])
        if self.response_plot_active:
            if self.response_plot_start_elapsed is not None:
                resp_elapsed = elapsed - self.response_plot_start_elapsed
            elif self.response_plot_t0 is not None:
                resp_elapsed = now_wall - self.response_plot_t0
            else:
                resp_elapsed = 0.0
            if self.response_plot_duration is not None and resp_elapsed > self.response_plot_duration:
                self.response_plot_active = False
            elif self.response_plot_end_time is None or now_wall <= self.response_plot_end_time:
                self.response_plot_times.append(resp_elapsed)
                self.response_plot_target.append(target)
                self.response_plot_actual.append(actual)
            else:
                self.response_plot_active = False

    def _parse_pid_status(self, line: str) -> bool:
        if not line.startswith("PID="):
            return False
        payload = line[4:]
        try:
            p_val, i_val, d_val = self._parse_pid_triplet(payload)
        except ValueError:
            return False
        self.current_p_var.set(f"{p_val:g}")
        self.current_i_var.set(f"{i_val:g}")
        self.current_d_var.set(f"{d_val:g}")
        return True

    def _parse_tune_status(self, line: str) -> bool:
        if not line.startswith("TUNE="):
            return False
        payload = line[5:]
        if payload.startswith("OK"):
            self.tune_status_var.set("Tune: OK")
            # Expected format: TUNE=OK,Ku=...,Pu=...,Kp=...,Ki=...,Kd=...
            vals = {}
            parts = payload.split(",")
            for part in parts[1:]:
                if "=" in part:
                    k, v = part.split("=", 1)
                    vals[k.strip()] = v.strip()
            if "Kp" in vals:
                self.p_var.set(vals["Kp"])
            if "Ki" in vals:
                self.i_var.set(vals["Ki"])
            if "Kd" in vals:
                self.d_var.set(vals["Kd"])
            self._log(f"RX: {line}")
            return True
        if payload.startswith("ERR"):
            self.tune_status_var.set("Tune: ERR")
            self._log(f"RX: {line}")
            return True
        if payload.startswith("START"):
            self.tune_status_var.set("Tune: RUNNING")
            self._log(f"RX: {line}")
            return True
        return False

    @staticmethod
    def _parse_pid_triplet(payload: str) -> tuple[float, float, float]:
        p_val = i_val = d_val = None
        parts = payload.split(",")
        for part in parts:
            if part.startswith("P="):
                p_val = float(part[2:].strip())
            elif part.startswith("I="):
                i_val = float(part[2:].strip())
            elif part.startswith("D="):
                d_val = float(part[2:].strip())
        if p_val is None or i_val is None or d_val is None:
            raise ValueError("missing PID fields")
        return p_val, i_val, d_val

    def _update_plot(self) -> None:
        if not self.plot_times:
            return
        self.target_line.set_data(self.plot_times, self.plot_target)
        self.actual_line.set_data(self.plot_times, self.plot_actual)
        self.axes.relim()
        self.axes.autoscale_view()
        self.canvas.draw_idle()
        if len(self.plot_times) >= 2:
            span = self.plot_times[-1] - self.plot_times[0]
            if span > 0:
                self.rx_rate_hz = (len(self.plot_times) - 1) / span
        if self.rx_rate_hz is None:
            self.rx_rate_hz = self.RX_RATE_HZ
        self.rx_rate_var.set(
            f"RX rate: {self.rx_rate_hz:.1f} Hz (Nyquist {self.rx_rate_hz/2:.1f} Hz)"
        )

    def _open_response_plot(self, duration: float | None) -> None:
        if self.response_plot_window is not None:
            try:
                self.response_plot_window.lift()
                self.response_plot_window.deiconify()
                self.response_plot_window.focus_force()
                return
            except tk.TclError:
                pass
        if self.response_plot_window is not None:
            try:
                self.response_plot_window.destroy()
            except tk.TclError:
                pass
        try:
            self.response_plot_window = tk.Toplevel(self.root)
        except tk.TclError as exc:
            self._log(f"ERR: failed to open response plot window: {exc}")
            self.response_plot_window = None
            return
        self.response_plot_window.title("Response Window")
        self.response_plot_window.geometry("700x400")
        self.response_plot_window.minsize(600, 350)
        self.response_plot_window.protocol("WM_DELETE_WINDOW", self._close_response_plot)
        self.response_plot_window.lift()
        self.response_plot_window.deiconify()
        self.response_plot_window.focus_force()
        self.response_plot_window.attributes("-topmost", True)
        self.response_plot_window.after(
            200, lambda: self.response_plot_window.attributes("-topmost", False)
        )
        self.response_plot_window.update_idletasks()

        figure = Figure(figsize=(5, 3), dpi=100)
        self.response_plot_axes = figure.add_subplot(111)
        self.response_plot_axes.set_xlabel("Time (s)")
        self.response_plot_axes.set_ylabel("Value")
        self.response_plot_axes.grid(True, alpha=0.3)
        self.response_plot_target_line, = self.response_plot_axes.plot([], [], label="Target")
        self.response_plot_actual_line, = self.response_plot_axes.plot([], [], label="Actual")
        self.response_plot_axes.legend(loc="upper right")

        self.response_plot_canvas = FigureCanvasTkAgg(figure, master=self.response_plot_window)
        self.response_plot_canvas.draw()
        self.response_plot_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.response_plot_canvas.mpl_connect("button_press_event", self._on_response_plot_click)
        self.response_plot_canvas.mpl_connect("motion_notify_event", self._on_response_plot_drag)
        self.response_plot_canvas.mpl_connect("button_release_event", self._on_response_plot_release)

        controls = ttk.Frame(self.response_plot_window, padding=(8, 4))
        controls.pack(fill=tk.X)
        self.response_plot_pause_button = ttk.Button(
            controls, text="Pause", command=self._toggle_response_plot_pause
        )
        self.response_plot_pause_button.pack(side=tk.LEFT)
        self.response_plot_clear_button = ttk.Button(
            controls, text="Clear Points", command=self._clear_response_point
        )
        self.response_plot_clear_button.pack(side=tk.LEFT, padx=6)
        self.response_cursor_a_button = ttk.Button(
            controls, text="Set Cursor A", command=lambda: self._toggle_active_cursor("A")
        )
        self.response_cursor_a_button.pack(side=tk.LEFT, padx=6)
        self.response_cursor_b_button = ttk.Button(
            controls, text="Set Cursor B", command=lambda: self._toggle_active_cursor("B")
        )
        self.response_cursor_b_button.pack(side=tk.LEFT, padx=6)
        self.response_plot_metrics_label = ttk.Label(
            controls, textvariable=self.response_metrics_var
        )
        self.response_plot_metrics_label.pack(side=tk.LEFT, padx=10)

        response_menubar = tk.Menu(self.response_plot_window)
        response_file_menu = tk.Menu(response_menubar, tearoff=0)
        response_file_menu.add_command(label="Save CSV", command=self._save_response_plot_data)
        response_file_menu.add_command(label="Save Image", command=self._save_response_plot_image)
        response_file_menu.add_separator()
        response_file_menu.add_command(label="Close", command=self._close_response_plot)
        response_menubar.add_cascade(label="File", menu=response_file_menu)
        self.response_plot_window.config(menu=response_menubar)

        self.response_plot_times.clear()
        self.response_plot_target.clear()
        self.response_plot_actual.clear()
        self.response_plot_paused = False
        self.response_plot_pause_button.configure(text="Pause")
        self.response_plot_annotations = []
        self.response_plot_markers = []
        self.response_point_dragging = False
        self.response_point_drag_index = None
        self.response_cursor_a = None
        self.response_cursor_b = None
        self.response_cursor_line_a = None
        self.response_cursor_line_b = None
        self.response_cursor_active = None
        self.response_cursor_dragging = None
        self._set_active_cursor(None)
        self.response_metrics_var.set("Cursors: --")
        self.response_plot_t0 = time.time()
        if duration is None:
            self.response_plot_end_time = None
            self.response_plot_duration = None
        else:
            self.response_plot_end_time = self.response_plot_t0 + duration
            self.response_plot_duration = duration
        self.response_plot_active = True
        self.response_plot_start_elapsed = self.plot_times[-1] if self.plot_times else None
        self._schedule_response_plot_update()

    def _set_response_plot_duration(self, duration: float) -> None:
        if self.response_plot_t0 is None:
            self.response_plot_t0 = time.time()
        self.response_plot_end_time = self.response_plot_t0 + duration
        self.response_plot_duration = duration

    def _open_response_plot_manual(self) -> None:
        try:
            duration = float(self.response_time_var.get().strip())
            if duration <= 0:
                raise ValueError
        except ValueError:
            duration = 5.0
        self._open_response_plot(duration)

    def _update_response_plot(self) -> None:
        if not self.response_plot_active or not self.response_plot_times:
            return
        self.response_plot_target_line.set_data(
            self.response_plot_times, self.response_plot_target
        )
        self.response_plot_actual_line.set_data(
            self.response_plot_times, self.response_plot_actual
        )
        self.response_plot_axes.relim()
        self.response_plot_axes.autoscale_view()
        self.response_plot_canvas.draw_idle()

    def _schedule_response_plot_update(self) -> None:
        if self.response_plot_after_id is not None:
            self.root.after_cancel(self.response_plot_after_id)
        if self.response_plot_active and not self.response_plot_paused:
            self._update_response_plot()
            # Throttle plot redraws to avoid UI stalls.
            self.response_plot_after_id = self.root.after(100, self._schedule_response_plot_update)
        else:
            self.response_plot_after_id = None

    def _close_response_plot(self) -> None:
        self.response_plot_active = False
        self.response_plot_end_time = None
        self.response_plot_t0 = None
        self.response_plot_start_elapsed = None
        self.response_plot_duration = None
        if self.response_plot_after_id is not None:
            try:
                self.root.after_cancel(self.response_plot_after_id)
            except tk.TclError:
                pass
        self.response_plot_after_id = None
        if self.response_plot_window is not None:
            try:
                self.response_plot_window.destroy()
            except tk.TclError:
                pass
        self.response_plot_window = None
        self.response_plot_canvas = None
        self.response_plot_axes = None
        self.response_plot_target_line = None
        self.response_plot_actual_line = None
        self.response_plot_pause_button = None
        self.response_plot_clear_button = None
        self.response_cursor_a_button = None
        self.response_cursor_b_button = None
        self.response_plot_metrics_label = None
        self.response_plot_annotations = []
        self.response_plot_markers = []
        self.response_point_dragging = False
        self.response_point_drag_index = None
        self.response_cursor_a = None
        self.response_cursor_b = None
        self.response_cursor_line_a = None
        self.response_cursor_line_b = None
        self.response_cursor_active = None
        self.response_cursor_dragging = None
        self.response_metrics_var.set("Cursors: --")

    def _toggle_response_plot_pause(self) -> None:
        if not self.response_plot_active:
            return
        self.response_plot_paused = not self.response_plot_paused
        if self.response_plot_pause_button is not None:
            self.response_plot_pause_button.configure(
                text="Resume" if self.response_plot_paused else "Pause"
            )
        if not self.response_plot_paused:
            self._schedule_response_plot_update()

    def _save_response_plot_data(self) -> None:
        if not self.response_plot_times:
            self._log("ERR: no response data to save.")
            return
        filepath = filedialog.asksaveasfilename(
            title="Save Response CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
        )
        if not filepath:
            return
        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["time_s", "target", "actual"])
                for t, target, actual in zip(
                    self.response_plot_times, self.response_plot_target, self.response_plot_actual
                ):
                    writer.writerow([f"{t:.6f}", f"{target:.6f}", f"{actual:.6f}"])
        except OSError as exc:
            self._log(f"ERR: failed to save response CSV: {exc}")
            return
        self._log(f"Saved response data: {filepath}")

    def _save_response_plot_image(self) -> None:
        if self.response_plot_canvas is None or self.response_plot_axes is None:
            self._log("ERR: response plot is not available.")
            return
        filepath = filedialog.asksaveasfilename(
            title="Save Response Image",
            defaultextension=".png",
            filetypes=[("PNG files", "*.png")],
        )
        if not filepath:
            return
        # Add metrics box to the figure for the saved image.
        metrics = self.response_metrics_var.get()
        box = self.response_plot_canvas.figure.text(
            0.01,
            0.01,
            metrics,
            ha="left",
            va="bottom",
            fontsize=9,
            bbox=dict(boxstyle="round", fc="white", ec="gray", alpha=0.9),
        )
        try:
            self.response_plot_canvas.figure.savefig(filepath, dpi=150, bbox_inches="tight")
        except OSError as exc:
            self._log(f"ERR: failed to save image: {exc}")
        finally:
            try:
                box.remove()
            except ValueError:
                pass
            self.response_plot_canvas.draw_idle()

    def _on_response_plot_click(self, event) -> None:
        if not self.response_plot_paused:
            return
        if event.inaxes != self.response_plot_axes:
            return
        if not self.response_plot_times:
            return
        if event.xdata is None:
            return
        if event.button == 3:
            self._remove_last_response_point()
            return
        # If click is near an existing point marker, start dragging it.
        idx = self._find_nearest_point_marker(event.xdata)
        if idx is not None:
            self.response_point_dragging = True
            self.response_point_drag_index = idx
            return
        # If click is near an existing cursor, start dragging it.
        tol = self._response_cursor_pick_tolerance()
        if self.response_cursor_a is not None and abs(event.xdata - self.response_cursor_a) <= tol:
            self.response_cursor_dragging = "A"
            return
        if self.response_cursor_b is not None and abs(event.xdata - self.response_cursor_b) <= tol:
            self.response_cursor_dragging = "B"
            return
        # Find nearest time index.
        x = event.xdata
        best_idx = 0
        best_dist = abs(self.response_plot_times[0] - x)
        for i in range(1, len(self.response_plot_times)):
            dist = abs(self.response_plot_times[i] - x)
            if dist < best_dist:
                best_dist = dist
                best_idx = i

        t = self.response_plot_times[best_idx]
        target = self.response_plot_target[best_idx]
        actual = self.response_plot_actual[best_idx]

        marker = self.response_plot_axes.plot(
            [t], [actual], "o", color="black", markersize=5
        )[0]
        text = f"t={t:.3f}s\nT={target:.3f}\nA={actual:.3f}"
        annotation = self.response_plot_axes.annotate(
            text,
            xy=(t, actual),
            xytext=(10, 10),
            textcoords="offset points",
            bbox=dict(boxstyle="round", fc="white", ec="gray", alpha=0.9),
        )
        self.response_plot_markers.append(marker)
        self.response_plot_annotations.append(annotation)
        # Update active cursor independently.
        if self.response_cursor_active == "A":
            self.response_cursor_a = t
        elif self.response_cursor_active == "B":
            self.response_cursor_b = t
        self._draw_response_cursors()
        self._update_response_metrics()
        self.response_plot_canvas.draw_idle()

    def _clear_response_point(self) -> None:
        for marker in self.response_plot_markers:
            try:
                marker.remove()
            except ValueError:
                pass
        for annotation in self.response_plot_annotations:
            try:
                annotation.remove()
            except ValueError:
                pass
        self.response_plot_markers = []
        self.response_plot_annotations = []
        if self.response_plot_canvas is not None:
            self.response_plot_canvas.draw_idle()

    def _remove_last_response_point(self) -> None:
        if not self.response_plot_markers or not self.response_plot_annotations:
            return
        marker = self.response_plot_markers.pop()
        annotation = self.response_plot_annotations.pop()
        try:
            marker.remove()
        except ValueError:
            pass
        try:
            annotation.remove()
        except ValueError:
            pass
        if self.response_plot_canvas is not None:
            self.response_plot_canvas.draw_idle()

    def _move_response_point_to_x(self, x: float, idx: int | None) -> None:
        if not self.response_plot_times:
            return
        if idx is None or idx < 0 or idx >= len(self.response_plot_markers):
            return
        best_idx = 0
        best_dist = abs(self.response_plot_times[0] - x)
        for i in range(1, len(self.response_plot_times)):
            dist = abs(self.response_plot_times[i] - x)
            if dist < best_dist:
                best_dist = dist
                best_idx = i

        t = self.response_plot_times[best_idx]
        target = self.response_plot_target[best_idx]
        actual = self.response_plot_actual[best_idx]

        try:
            self.response_plot_markers[idx].set_data([t], [actual])
        except ValueError:
            pass
        try:
            text = f"t={t:.3f}s\nT={target:.3f}\nA={actual:.3f}"
            self.response_plot_annotations[idx].set_text(text)
            self.response_plot_annotations[idx].xy = (t, actual)
        except (IndexError, ValueError):
            pass
        self.response_plot_canvas.draw_idle()

    def _find_nearest_point_marker(self, x: float) -> int | None:
        if not self.response_plot_markers:
            return None
        tol = self._response_cursor_pick_tolerance()
        best_idx = None
        best_dist = None
        for i, marker in enumerate(self.response_plot_markers):
            try:
                mx = marker.get_xdata()[0]
            except (IndexError, TypeError):
                continue
            dist = abs(x - mx)
            if dist <= tol and (best_dist is None or dist < best_dist):
                best_dist = dist
                best_idx = i
        return best_idx

    def _on_response_plot_drag(self, event) -> None:
        if not self.response_plot_paused:
            return
        if self.response_point_dragging:
            if event.inaxes != self.response_plot_axes or event.xdata is None:
                return
            self._move_response_point_to_x(event.xdata, self.response_point_drag_index)
            return
        if self.response_cursor_dragging is None:
            return
        if event.inaxes != self.response_plot_axes:
            return
        if event.xdata is None:
            return
        if self.response_cursor_dragging == "A":
            self.response_cursor_a = event.xdata
        else:
            self.response_cursor_b = event.xdata
        self._draw_response_cursors()
        self._update_response_metrics()
        self.response_plot_canvas.draw_idle()

    def _on_response_plot_release(self, event) -> None:
        if self.response_cursor_dragging is not None:
            self.response_cursor_dragging = None
        if self.response_point_dragging:
            self.response_point_dragging = False
            self.response_point_drag_index = None

    def _response_cursor_pick_tolerance(self) -> float:
        if self.response_plot_axes is None:
            return 0.0
        x_min, x_max = self.response_plot_axes.get_xlim()
        return max((x_max - x_min) * 0.01, 0.01)

    def _toggle_active_cursor(self, which: str) -> None:
        if self.response_cursor_active == which:
            self._set_active_cursor(None)
        else:
            self._set_active_cursor(which)

    def _set_active_cursor(self, which: str | None) -> None:
        self.response_cursor_active = which
        if self.response_cursor_a_button is not None and self.response_cursor_b_button is not None:
            if which == "A":
                self.response_cursor_a_button.state(["pressed"])
                self.response_cursor_b_button.state(["!pressed"])
            elif which == "B":
                self.response_cursor_a_button.state(["!pressed"])
                self.response_cursor_b_button.state(["pressed"])
            else:
                self.response_cursor_a_button.state(["!pressed"])
                self.response_cursor_b_button.state(["!pressed"])

    def _draw_response_cursors(self) -> None:
        if self.response_cursor_line_a is not None:
            try:
                self.response_cursor_line_a.remove()
            except ValueError:
                pass
        if self.response_cursor_line_b is not None:
            try:
                self.response_cursor_line_b.remove()
            except ValueError:
                pass
        self.response_cursor_line_a = None
        self.response_cursor_line_b = None

        if self.response_cursor_a is not None:
            self.response_cursor_line_a = self.response_plot_axes.axvline(
                self.response_cursor_a, color="tab:blue", linestyle="--", linewidth=1.0
            )
        if self.response_cursor_b is not None:
            self.response_cursor_line_b = self.response_plot_axes.axvline(
                self.response_cursor_b, color="tab:orange", linestyle="--", linewidth=1.0
            )

    def _update_response_metrics(self) -> None:
        if self.response_cursor_a is None or self.response_cursor_b is None:
            self.response_metrics_var.set("Cursors: set A and B")
            return
        t_start = min(self.response_cursor_a, self.response_cursor_b)
        t_end = max(self.response_cursor_a, self.response_cursor_b)
        if t_end <= t_start:
            self.response_metrics_var.set("Cursors: invalid range")
            return
        # Extract windowed samples.
        idx = [
            i for i, t in enumerate(self.response_plot_times)
            if t_start <= t <= t_end
        ]
        if not idx:
            self.response_metrics_var.set("Cursors: no data")
            return
        targets = [self.response_plot_target[i] for i in idx]
        actuals = [self.response_plot_actual[i] for i in idx]
        times = [self.response_plot_times[i] for i in idx]
        target = statistics.median(targets)

        # Estimate previous target from a short window before cursor A.
        prev_idx = [
            i for i, t in enumerate(self.response_plot_times)
            if (t_start - 0.2) <= t < t_start
        ]
        if prev_idx:
            prev_targets = [self.response_plot_target[i] for i in prev_idx]
            prev_target = statistics.median(prev_targets)
        else:
            prev_target = target

        step_size = abs(target - prev_target)
        if step_size > 0:
            direction = 1.0 if target >= prev_target else -1.0
            overshoot_val = max(direction * (a - target) for a in actuals)
            overshoot_val = max(overshoot_val, 0.0)
            overshoot_pct = (overshoot_val / step_size) * 100.0
            overshoot_text = f"{overshoot_pct:.2f}%"
        else:
            overshoot_text = "--"

        # Peak and rise time (10%-90%).
        peak_val = max(actuals) if actuals else None
        peak_time = times[actuals.index(peak_val)] if actuals else None
        if peak_val is not None and peak_time is not None:
            peak_text = f"{peak_val:.3f} @ {peak_time - t_start:.3f}s"
        else:
            peak_text = "--"

        rise_text = "--"
        if step_size > 0:
            lo = prev_target + 0.1 * (target - prev_target)
            hi = prev_target + 0.9 * (target - prev_target)
            t_lo = t_hi = None
            for t, a in zip(times, actuals):
                if t_lo is None:
                    if (target >= prev_target and a >= lo) or (target < prev_target and a <= lo):
                        t_lo = t
                if t_hi is None:
                    if (target >= prev_target and a >= hi) or (target < prev_target and a <= hi):
                        t_hi = t
                if t_lo is not None and t_hi is not None:
                    break
            if t_lo is not None and t_hi is not None and t_hi >= t_lo:
                rise_text = f"{(t_hi - t_lo):.3f}s"

        band = max(abs(target) * 0.02, 0.01)
        settle_time = None
        # Cursor-based settling: first time after which the response stays within band
        # until the end of the cursor window.
        for i, t0 in enumerate(times):
            in_band = True
            for a in actuals[i:]:
                if abs(a - target) > band:
                    in_band = False
                    break
            if in_band:
                settle_time = t0 - t_start
                break
        settle_text = f"{settle_time:.3f}s" if settle_time is not None else "--"

        # SSE: average over last 20% of window (min 0.1s).
        window_len = t_end - t_start
        tail = max(window_len * 0.2, 0.1)
        sse_start = max(t_start, t_end - tail)
        sse_vals = [
            a for t, a in zip(times, actuals) if sse_start <= t <= t_end
        ]
        if sse_vals:
            sse = target - (sum(sse_vals) / len(sse_vals))
            if self.sse_percent_var.get() and target != 0:
                sse_text = f"{(sse / target) * 100.0:.2f}%"
            else:
                sse_text = f"{sse:.4f}"
        else:
            sse_text = "--"

        self.response_metrics_var.set(
            f"Settling: {settle_text} | Rise: {rise_text} | Peak: {peak_text} | "
            f"%OS: {overshoot_text} | SSE: {sse_text}"
        )

    def _start_step_metrics(
        self, target: float, start_time: float | None = None, prev_target: float | None = None
    ) -> None:
        self.step_active = True
        self.step_target = target
        self.step_prev_target = prev_target
        self.step_start_time = start_time
        self.settle_start_time = None
        self.settled_time = None
        self.overshoot_max = 0.0
        self.settling_time_var.set("--")
        self.overshoot_var.set("0.00")
        self.sse_var.set("--")
        self.actual_history.clear()

    def _update_step_metrics(self, actual: float, now: float) -> None:
        if not self.step_active or self.step_target is None or self.step_start_time is None:
            return

        target = self.step_target
        if self.step_prev_target is not None:
            direction = 1.0 if target >= self.step_prev_target else -1.0
            step_size = abs(target - self.step_prev_target)
        else:
            direction = 1.0 if target >= 0 else -1.0
            step_size = abs(target)
        overshoot_val = direction * (actual - target)
        if overshoot_val > self.overshoot_max:
            self.overshoot_max = overshoot_val
            if step_size > 0:
                overshoot_pct = (self.overshoot_max / step_size) * 100.0
                self.overshoot_var.set(f"{overshoot_pct:.2f}")
            else:
                self.overshoot_var.set("--")

        band = max(abs(target) * 0.02, 0.01)
        if abs(actual - target) <= band:
            if self.settle_start_time is None:
                self.settle_start_time = now
            elif now - self.settle_start_time >= 2.0:
                if self.settled_time is None:
                    self.settled_time = now
                    settling_time = self.settled_time - self.step_start_time
                    self.settling_time_var.set(f"{settling_time:.2f}")
        else:
            self.settle_start_time = None

        if self.settled_time is not None and now - self.settled_time >= 2.0:
            avg_actual = self._avg_actual(now - 2.0, now)
            if avg_actual is not None:
                sse = target - avg_actual
                self._set_sse_value(sse)

    def _trim_history(self, now: float) -> None:
        cutoff = now - 10.0
        while self.actual_history and self.actual_history[0][0] < cutoff:
            self.actual_history.popleft()

    def _avg_actual(self, start_time: float, end_time: float) -> float | None:
        values = [val for ts, val in self.actual_history if start_time <= ts <= end_time]
        if not values:
            return None
        return sum(values) / len(values)

    def _set_sse_value(self, sse: float) -> None:
        target = self.step_target
        if target is None:
            self.sse_var.set("--")
            return
        if target == 0:
            if self.sse_percent_var.get():
                self.sse_var.set("--")
            else:
                self.sse_var.set(f"{sse:.4f}")
            return
        if self.sse_percent_var.get():
            self.sse_var.set(f"{(sse / target) * 100.0:.2f}")
        else:
            self.sse_var.set(f"{sse:.4f}")

    def _refresh_sse_display(self) -> None:
        if not self.step_active or self.step_target is None:
            return
        now = time.time()
        if self.settled_time is None or now - self.settled_time < 2.0:
            return
        avg_actual = self._avg_actual(now - 2.0, now)
        if avg_actual is None:
            return
        sse = self.step_target - avg_actual
        self._set_sse_value(sse)

    def _toggle_plot(self) -> None:
        pane_ids = self.paned.panes()
        plot_id = str(self.plot_frame)
        if self.show_plot_var.get():
            if plot_id not in pane_ids:
                self.paned.add(self.plot_frame, weight=3)
        else:
            if plot_id in pane_ids:
                self.paned.forget(self.plot_frame)

    def _toggle_log(self) -> None:
        pane_ids = self.paned.panes()
        log_id = str(self.log_frame)
        if self.show_log_var.get():
            if log_id not in pane_ids:
                if not pane_ids:
                    self.paned.add(self.log_frame, weight=2)
                else:
                    self.paned.insert(0, self.log_frame, weight=2)
        else:
            if log_id in pane_ids:
                self.paned.forget(self.log_frame)

    def _on_close(self) -> None:
        if self.recording:
            self._stop_recording()
        if self.serial_port:
            self._disconnect()
        self.root.destroy()

    def _start_recording(self) -> None:
        if self.recording:
            return

        filepath = filedialog.asksaveasfilename(
            title="Save CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
        )
        if not filepath:
            return

        try:
            self.csv_file = open(filepath, "w", newline="", encoding="utf-8")
        except OSError as exc:
            self._log(f"ERR: failed to open CSV: {exc}")
            self.csv_file = None
            return

        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(["timestamp", "target", "actual"])
        self.recording = True
        self.record_button.configure(state="disabled")
        self.stop_button.configure(state="normal")
        self._log(f"Recording started: {filepath}")

    def _stop_recording(self) -> None:
        if not self.recording:
            return

        self.recording = False
        if self.csv_file:
            self.csv_file.close()
        self.csv_file = None
        self.csv_writer = None
        self.record_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self._log("Recording stopped.")


def main() -> None:
    root = tk.Tk()
    ttk.Style().theme_use("clam")
    CdcGuiApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
