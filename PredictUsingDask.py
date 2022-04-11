from dask.distributed import Client
import dask.bag as db

def run_ml_algorithem(record_id):
  # process the record and run the ml algorithem
  

if __name__ == '__main__':
  
    client = Client(processes=True)
    
    record_list = get_record_list()
    
    proc_bag = db.from_sequence(scan_windows, npartitions=100).map(run_ml_algorithem)
    
    proc_bag.compute()
