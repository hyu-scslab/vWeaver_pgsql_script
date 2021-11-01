# pip install psycopg2
import psycopg2

import sys, subprocess
import time, threading
import os, random
import argparse
import pathlib, datetime
import json
import shutil

# 0 for MySQL, 1 for PostgreSQL
RUN_MODE=1
# 0 for Vanilla, 1 for Ours
RUN_TYPE=0
# Prevent compile
SYSBENCH_COMP=False

# Non sysbench options
NON_SYSBENCH=["run_mode", "compile_option", "num_short_olap", \
        "num_long_olap", "olap_wait_time",]
BEGIN_TIME=0

MYSQL=0
POSTGRESQL=1
BOTH=2

RESULT_DIR=""
RESULT_BASE=""

STOPPED_OLAP = False

COOLING_TIME=0

SHORT_TABLE_IDX=0
SHORT_TABLE_NUM=0

LONG_TABLE_IDX=0
LONG_TABLE_NUM=0

# Script should be located in proper directory based on below path
DB_BASE=["./MySQL/", "./PostgreSQL/"]

# DB directroy ...
DB_DATA=[base + "data/" for base in DB_BASE] 
DB_CONFIG=[base + "config/" for base in DB_BASE]

# DB script ...
DB_SCRIPT=[base + "script/" for base in DB_BASE]
DB_SERVER_SCRIPT=[base + "script_server/" for base in DB_SCRIPT]
DB_CLIENT_SCRIPT=[base + "script_client/" for base in DB_SCRIPT]
DB_INSTALL_SCRIPT=[base + "script_install/" for base in DB_SCRIPT]

# Sysbench ...
SYSBENCH_BASE=[base + "sysbench/" for base in DB_BASE]
SYSBENCH=[base + "sysbench/src/sysbench" for base in SYSBENCH_BASE]
SYSBENCH_LUA=[base + "sysbench/src/lua/" for base in SYSBENCH_BASE]
SYSBENCH_SCRIPT=[base + "sysbench_script/" for base in SYSBENCH_BASE]

class SysbenchWorker(threading.Thread):
    def __init__(self, args):
        threading.Thread.__init__(self)
        self.opts, self.workload = self.setting_params(args)
        self.current_dir = os.getcwd() + "/"

        if RUN_TYPE == 0:
            self.result_file = open(RESULT_DIR + "pgsql_sysbench_vanilla_result", 'w')
        else:
            self.result_file = open(RESULT_DIR + "pgsql_sysbench_vweaver_result", 'w')

        print("Sysbench cleanup & prepare start")
        os.system("cd " + SYSBENCH_LUA[RUN_MODE] + "; " + \
                self.current_dir + SYSBENCH[RUN_MODE] + " " + self.opts + \
                " " + self.workload + " cleanup > /dev/null")
        os.system("cd " + SYSBENCH_LUA[RUN_MODE] + "; " + \
                self.current_dir + SYSBENCH[RUN_MODE] + " " + self.opts + \
                " " + self.workload + " prepare > /dev/null")

        print("Sysbench cleanup & prepare finish")

    def run(self):
        print("Sysbench worker start !")
        subprocess.run(args=[ 
            self.current_dir + SYSBENCH[RUN_MODE] + " " +  self.opts + \
            " " + self.workload +" run"],
            stdout=self.result_file, stderr=subprocess.STDOUT, check=True,
            cwd=SYSBENCH_LUA[RUN_MODE], shell=True)
        print("Sysbench worker end !")
        self.result_file.close()

    def setting_params(self, args):
        opts=""
        for k, v in vars(args).items():
            if k is "workload":
                continue
            if RUN_MODE is MYSQL and "pgsql" in k:
                continue
            if RUN_MODE is POSTGRESQL and "mysql" in k:
                continue
            # Except non-sysbench opts
            if k in NON_SYSBENCH:
                continue
            opts += " --" + k + "=" + v

        workload=args.workload
        return opts, workload

class Client(threading.Thread):
    def __init__(self, long_trx, client_no, args):
        threading.Thread.__init__(self)
        self.client_no = client_no
        self.num_tables = int(args.tables)
        self.long_trx = long_trx
        self.args = args
        if long_trx != 2:
            if RUN_TYPE == 0:
                self.result_file = \
                        open(RESULT_DIR + "pgsql_vanilla_client_" + \
                        str(self.long_trx) + "_" + str(client_no), 'w')
            else:
                self.result_file = \
                        open(RESULT_DIR + "pgsql_vweaver_client_" + \
                        str(self.long_trx) + "_" + str(client_no), 'w')

        self.make_connector()
        if long_trx == 0:
            self.query = self.make_short_query()
        elif long_trx == 1:
            self.query = self.make_long_query()
        else:
            self.query = self.make_update_query()
        self.prepare_to_run()

    def prepare_to_run(self):
      '''
        For now, all records visible to the transaction should be in the undo log.
        Therefore, it makes its ReadView firt, and then
        update client updates all records once before executing a join query.
      '''
      if self.long_trx != 1:
        return
      self.db.autocommit=False
      self.cursor.execute("SELECT id FROM sbtest1 where id=1;")
      self.cursor.fetchall()

    def run(self):
        if self.long_trx != 1:
          self.db.autocommit = True

        if self.long_trx == 2:
          # Update
          self.cursor.execute(self.query)
          self.cursor.close()
          self.db.close()
          return
      
        print ("Client " + str(self.client_no) + " start !")
 
        while STOPPED_OLAP == False:
            start_time = time.perf_counter()
            self.cursor.execute(self.query)
            self.cursor.fetchall()
            end_time = time.perf_counter()
            results = f'{(start_time - BEGIN_TIME):.3f} {(end_time - BEGIN_TIME):.3f}\n'
            self.result_file.write(results)
            self.result_file.flush()

        if self.long_trx == 1:
            self.db.commit()

        self.cursor.close()
        self.db.close()

        print("Client " + str(self.long_trx) + "_" + str(self.client_no) + " end !")
        self.result_file.close()

    def make_short_query(self):
        global SHORT_TABLE_IDX
        global SHORT_TABLE_NUM
        '''
        Short transaction query
        Number of tables : 2, 3, 4, 5
        Round-robin manner
        '''
        n_tables = SHORT_TABLE_NUM + 2
        SHORT_TABLE_NUM = (SHORT_TABLE_NUM + 1) % 4

        random_table_numbers = [ (SHORT_TABLE_IDX + i) % self.num_tables + 1 for i in range(0, n_tables)]
        SHORT_TABLE_IDX += n_tables

        random_table_numbers.sort()

        from_stmt = ["sbtest" + str(i) for i in random_table_numbers]
        where_stmt = [i + ".id" for i in from_stmt]
        first_table_id = where_stmt[0]

        #where_stmt = [i + ".k" for i in from_stmt]
        select_stmt = [i + ".k" for i in from_stmt]
        select_stmt = '+'.join(select_stmt)
        select_stmt = 'SUM(' + select_stmt + ')'

        from_stmt = ', '.join(from_stmt)

        where_stmt = \
                [where_stmt[i] + "=" + where_stmt[i+1] \
                for i in range(len(where_stmt) - 1)]

        where_stmt = ' and '.join(where_stmt) + ' and (' + first_table_id + '%2)=0'
        #where_stmt = ' and '.join(where_stmt)
        query = "SELECT " + select_stmt + " FROM " + from_stmt + \
                " WHERE " + where_stmt + ";"
       
        #query = "SELECT COUNT(*) FROM " + from_stmt + \
        #        " WHERE " + where_stmt + ";"

        return query

    def make_long_query(self):
        global LONG_TABLE_IDX
        global LONG_TABLE_NUM

        '''
        Long transaction query
        Number of tables : 
        '''
        table_numbers = [ (i + 1) for i in range(0, self.num_tables) ]

        from_stmt = ["sbtest" + str(i) for i in table_numbers]

        where_stmt = [i + ".id" for i in from_stmt]
        first_table_id = where_stmt[0]

        select_stmt = [i + ".k" for i in from_stmt]
        select_stmt = '+'.join(select_stmt)
        select_stmt = 'SUM(' + select_stmt + ')'
        from_stmt = ', '.join(from_stmt)

        where_stmt = \
                [where_stmt[i] + "=" + where_stmt[i+1] \
                for i in range(len(where_stmt) - 1)]
        where_stmt = ' and '.join(where_stmt) + ' and (' + first_table_id + '%2)=0'

        query = "SELECT " + select_stmt + " FROM " + from_stmt + \
                " WHERE " + where_stmt + ";"
        return query

    def make_update_query(self):
        table_numbers = [ "sbtest" + str(i+1) for i in range(0, self.num_tables) ]

        query = [ "UPDATE " + i + " SET k=k+1" for i in table_numbers ]

        query = ";".join(query)
        return query + ";"

    def make_connector(self):
        self.db = psycopg2.connect(
                host=self.args.pgsql_host,
                dbname=self.args.pgsql_db,
                user=self.args.pgsql_user,
                port=self.args.pgsql_port
                )
        self.cursor = self.db.cursor()

def init_sysbench():
    print("Compile sysbench start")
    subprocess.run(args=[SYSBENCH_SCRIPT[RUN_MODE]+"install.sh"],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
            check=True)
    print("Compile sysbench finish")

def create_db():
    print("Create db start")
    subprocess.run(args=[DB_CLIENT_SCRIPT[RUN_MODE]+"create_db.sh"],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
            check=True)
    print("Create db finish")

def compile_database(params=None):
    print("Compile database start !")
    subprocess.run(args=[DB_INSTALL_SCRIPT[RUN_MODE]+"install.sh",
        params], 
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True)
    print("Compile database finish !")

def init_server(params=None):
    print("Init server start !")
    subprocess.run(args=[DB_SERVER_SCRIPT[RUN_MODE]+"init_server.sh",
        params],
        check=True)
    print("Init server finish !")

def run_server(params=None):
    print("Run server start")
    subprocess.run(args=[DB_SERVER_SCRIPT[RUN_MODE]+"run_server.sh",
        params], 
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True)
    time.sleep(20)
    print("Run server finish")

def shutdown_server(params=None):
    print("Shutdown server start")
    subprocess.run(args=[DB_SERVER_SCRIPT[RUN_MODE]+"shutdown_server.sh",
        params], 
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True)
    time.sleep(30)
    print("Shutdown server finish")

def run_standard_benchmark_pgsql(args, v_flag):
    global BEGIN_TIME
    global RESULT_DIR
    global STOPPED_OLAP
    global STOPPED_SPACE
    global RUN_TYPE
    global SYSBENCH_COMP
    global RESULT_DIR
    global SHORT_TABLE_IDX
    global SHORT_TABLE_NUM
    global LONG_TABLE_NUM
    global LONG_TABLE_IDX

    if v_flag is True:
        print("PostgreSQL-vweaver benchmark start")
    else:
        print("PostgreSQL-vanilla benchmark start")

    if RESULT_DIR == "":
      RESULT_DIR = "./results/pgsql_" + datetime.datetime.utcnow().strftime( \
              "%y-%m-%d_%H:%M:%S") + "/"
  
      pathlib.Path(RESULT_DIR).mkdir(parents=True, exist_ok=True)
  
      with open(RESULT_DIR + "arg_list", 'w') as f:
          json.dump(args.__dict__, f, indent=2)

    # Compile sysbench code
    if SYSBENCH_COMP is False:
      init_sysbench()
      SYSBENCH_COMP = True

    if v_flag is True:
      RUN_TYPE = 1
    else:
      RUN_TYPE = 0

    SHORT_TABLE_IDX=0
    SHORT_TABLE_NUM=0

    LONG_TABLE_IDX=0
    LONG_TABLE_NUM=0

    STOPPED_OLAP=False

    if v_flag is True:
      compile_database(params=args.compile_option)
    else:
      compile_database(params="")

    init_server(params="")

    # copy postgresql.conf to data dir..
    shutil.copy(DB_BASE[RUN_MODE] + "postgresql.conf-std", DB_BASE[RUN_MODE] + "data/postgresql.conf")

    run_server(params="")
    
    # create sbtest database
    create_db()
    # Make sysbench workers
    sys_worker = SysbenchWorker(args)

    # Make clients (reader)
    olap_client = [Client(long_trx=1, client_no=i, args=args) for i in range(0, args.num_long_olap)]

    # Do updates all tables once !
    print("Start updating all records")
    update_client = Client(long_trx=2, client_no=0, args=args)
    update_client.start()
    update_client.join()
    print("Finish updating all records")

    # Run sysbench worker
    sys_worker.start()
    # wait for warmup time if needed
    if int(args.warmup_time) != 0:
        time.sleep(int(args.warmup_time))

    BEGIN_TIME = time.perf_counter()

    time.sleep(int(args.olap_wait_time)) # Default: olap_wait_time: 0 sec

    print("Start client workers")
    for client in olap_client:
        client.start()

    # Third phase : Run OLAP ONLY
    while time.perf_counter() - BEGIN_TIME < float(args.time):
        time.sleep(0.5)

    STOPPED_OLAP = True

    for client in olap_client:
        client.join()

    sys_worker.join()
    shutdown_server(params="")
    if v_flag is True:
      print("PostgreSQL-vweaver benchmark done")
    else:
      print("PostgreSQL-vanilla benchmark done")

    time.sleep(10)

if __name__ == "__main__":
    # Parser
    parser = argparse.ArgumentParser(description="Sysbench Arguments...")

    sysbc_parser = parser.add_argument_group('sysbench', 'sysbench options')
    pgsql_parser = parser.add_argument_group('pgsql', 'postgresql options')
    options_parser = parser.add_argument_group('options', 'other options')

    options_parser.add_argument("--run_mode", type=int, default=1, help="0: mysql, 1: postgresql, 2: both")
    options_parser.add_argument("--compile_option", default="-DVWEAVER", help="compile options")

    options_parser.add_argument("--num_long_olap", type=int, default=1, help="compile options")

    options_parser.add_argument("--olap_wait_time", type=int, default=0, help="compile options")

    sysbc_parser.add_argument("--db-driver", default="pgsql", help="mysql or pgsql")

    pgsql_parser.add_argument("--pgsql-host", default="localhost", help="pgsql host")
    pgsql_parser.add_argument("--pgsql-db", default="sbtest", help="pgsql database")
    pgsql_parser.add_argument("--pgsql-user", default="sbtest", help="pgsql user")
    pgsql_parser.add_argument("--pgsql-port", default="9988", help="pgsql port number")

    sysbc_parser.add_argument("--report-interval", default="1", help="report interval. default 1 seconod")
    sysbc_parser.add_argument("--secondary", default="off", help="default off")
    sysbc_parser.add_argument("--create-secondary", default="false", help="default = false")
    sysbc_parser.add_argument("--time", default="20", help="total execution time")
    sysbc_parser.add_argument("--threads", default="12", help="number of threads")
    sysbc_parser.add_argument("--tables", default="12", help="number of tables")
    sysbc_parser.add_argument("--table-size", default="10000", help="sysbench table size")
    sysbc_parser.add_argument("--warmup-time", default="0", help="sysbench warmup time")
    sysbc_parser.add_argument("--rand-type", default="zipfian")
    sysbc_parser.add_argument("--rand-zipfian-exp", default="0.0")
    sysbc_parser.add_argument("--workload", default="oltp_update_non_index.lua",\
            help="lua script. default : oltp update non index")
    args=parser.parse_args()
    run_standard_benchmark_pgsql(args, False)
    run_standard_benchmark_pgsql(args, True)
