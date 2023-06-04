## Script to getting backup/restore from postgresql

# Setup:
    1) Install dependencies
       pip3 install -r requirements.txt
    
    2) Create configuration file (.env) --> host=<your_psql_addr(probably 127.0.0.1)>
                                            port=<your_psql_port(probably 5432)>
                                            db=<your_db_name>
                                            user=<your_username>
                                            password=<your_password>

# Usage:
    Create database backup and store it (based on config file details):
          python3 manage_database.py --action backup 
    
    List backups available on storage 
          python3 manage_database.py --action list 

    Restore backups available on storage (check available dates with listaction):
          python3 manage_database.py --action restore 
    
    Switching restored database with active one:
          python3 manage_database.py --action active 

