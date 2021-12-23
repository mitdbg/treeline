import enum
import ycsbr_py as ycsbr


def extract_workload(workload_config_file, record_size_bytes):
    db = _WorkloadExtractor()
    print("Loading workload...")
    workload = ycsbr.PhasedWorkload.from_file(workload_config_file, set_record_size_bytes=record_size_bytes)
    print("Starting session...")
    session = ycsbr.Session(num_threads=1)
    session.set_database(db)
    session.initialize()
    try:
        print("Running bulk load...")
        session.replay_bulk_load_trace(workload.get_load_trace())
        print("Running workload...")
        session.run_phased_workload(workload)
        print("Done running workload...")
        return db
    finally:
        session.terminate()


def extract_trace(workload_config_file):
    db = _TraceExtractor()
    workload = ycsbr.PhasedWorkload.from_file(workload_config_file, set_record_size_bytes=16)
    session = ycsbr.Session(num_threads=1)
    session.set_database(db)
    session.initialize()
    try:
        session.run_phased_workload(workload)
        return db.trace
    finally:
        session.terminate()


class _WorkloadExtractor(ycsbr.DatabaseInterface):
    """A YCSBR database interface that records the workload's load dataset, read key trace, and update key trace."""

    def __init__(self):
        ycsbr.DatabaseInterface.__init__(self)
        self.dataset = []
        self.update_trace = []
        self.read_trace = []

    def initialize_database(self):
        pass

    def shutdown_database(self):
        pass

    def bulk_load(self, load):
        for i in range(len(load)):
            self.dataset.append(load.get_key_at(i))

    def insert(self, key, val):
        return True

    def update(self, key, val):
        self.update_trace.append(key)
        return True

    def read(self, key):
        self.read_trace.append(key)
        return None

    def scan(self, start, amount):
        return []


class Op(enum.Enum):
    Read = 1
    Update = 2


class _TraceExtractor(ycsbr.DatabaseInterface):
    def __init__(self):
        ycsbr.DatabaseInterface.__init__(self)
        self.trace = []

    def initialize_database(self):
        pass

    def shutdown_database(self):
        pass

    def bulk_load(self, load):
        pass

    def insert(self, key, val):
        return True

    def update(self, key, val):
        self.trace.append((Op.Update, key))
        return True

    def read(self, key):
        self.trace.append((Op.Read, key))
        return None

    def scan(self, start, amount):
        return []
