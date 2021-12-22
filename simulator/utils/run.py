import ycsbr_py as ycsbr


def run_workload(workload, db):
    session = ycsbr.Session(num_threads=1)
    session.set_database(db)
    session.initialize()
    try:
        session.run_phased_workload(workload)
        return db
    finally:
        session.terminate()
