#!/usr/bin/env python

app_version="1.23 test"

# (c) 2014-2015 LINBEDDED Pawel Suchanecki
#               pawel.suchanecki@gmail.com

try:
    import socket, threading, signal, sys, time, sqlite3 as lite
    from threading import Thread, Lock
    import pyodbc
    import re
    import logging
    import wmi
    import ctypes
    import os
    import unicodedata
    import encodings.utf_8
    import encodings.cp1250
    import configparser
except ImportError:
    pass

binary_name="Linbedded-SQL-Server.exe"

# chceck if not running already
c = wmi.WMI ()
process_cnt=0
_processes=[]

for process in c.Win32_Process ():
    if (binary_name in process.Name):
        process_cnt+=1
        _processes.append(process.ProcessId)

if (process_cnt > 1):
    this_pid=os.getpid()
    for pid in range(len(_processes)):
        if this_pid != pid:
            msgbox = ctypes.windll.user32.MessageBoxA
            msg="ERROR: serwer poprzednio uruchomiony jako proces o PID #" + str(pid) + "\nZamknij proces Linbedded-SQL-Server.exe przez Process Manager'a Windows."
            ret = msgbox(None, msg, 'Linbedded SQL SERVER', 0)
            if ret == 1:
                print("Pressed OK")
            elif ret == 2:
                print("Pressed Cancel")
    sys.exit(1)


db_path = "C:\\Linbedded-SQL-SERVER\DBS\\"
log_path = "C:\\Linbedded-SQL-SERVER\LOGS\\"
host = "0.0.0.0"
svr_port = 9999
db_backend = 'MSSQL'
SVR_ID = "LINBEDDED/Linbedded SQL PROXY SERVER v " + str(app_version) + " " + str(db_backend) + " utf8 support"

#db_backend = "SQLITE"
DSN_con_string=""

# MS SQL
#MSSQL_directive_NO_COUNT = """
#SET NOCOUNT ON;
#
#"""

MSSQL_directive_NO_COUNT=""

# config: TODO: use configparser to do this right
try:
    conf = open ('C:\\Linbedded-SQL-SERVER\\config.dat', 'r')
    for line in conf:
        DSN_con_string = line.rstrip('\n')
except Exception, e:
    print "config open error:" + str(e)



# logger
#logging.basicConfig(level=logging.INFO,
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    filename=time.strftime(log_path+"Linbedded-SQL-Server-log-%Y-%m-%d--%H-%M.txt"),
                    filemode='w')

logger = logging.getLogger('Linbedded-SQL-SERVER')

logger.info('log starts')
logger.info('server version: '+ SVR_ID)
logger.info('read DSN connection string: ' + str(DSN_con_string))

client_id = 0

tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

tcpsock.bind((host, svr_port))
threads = []

mutex = Lock()


globals()["run_flag"] = True
original_sigbreak = 0

class NameSpace : pass

class ClientThread(threading.Thread):

    def __init__(self,ip,port,socket,p_client_id):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.socket = socket
        self.client_id = p_client_id
        logger.info("new thread started for client id="+str(self.client_id)+" "+ip+":"+str(self.port))

    def run(self):
        logger.info("connection from: "+self.ip+":"+str(self.port))

        iteration  = 0

        data = "dummydata"

        while len(data) > 1:
            iteration+=1
            data = clientsock.recv(2048)
            #print "Client @ ip/port: "+self.ip+":"+str(self.port)+") sent (iter:"+str(iteration)+"): '"+data+"'"
            logger.info("Client @ ip/port: "+self.ip+":"+str(self.port)+") sent (iter:"+str(iteration)+"): '"+data+"'")

            logger.debug("DBG built-in commands parser starts")

            if data == 'HELO':
                # IDENTIFY
                SVR_HOSTNAME=socket.gethostname()
                SVR_IP=socket.gethostbyname(SVR_HOSTNAME)
                self.socket.send(SVR_ID+" @ "+SVR_IP+":"+str(svr_port)+" ("+SVR_HOSTNAME+") :: DB_OK\n")
                continue

            # EXIT CMD
            if data == 'CMD_EXIT':
                self.socket.send("\nExit request!")
                # thread exits
                self.socket.close();
                globals()["run_flag"] = False
                exit()

            logger.debug("DBG built-in commands parser ends")

            if (db_backend == 'SQLITE'):
                # DB SQLITE CODE
                m = re.search ('BASE:(.*)#(.*)', str(data))

                if (m != None):
                    SQL_BASE=m.group(1)
                    SQL_CMD=m.group(2)

                    #print "SQL_BASE = " + SQL_BASE
                    #print "SQL_CMD  = " + SQL_CMD

                    mutex.acquire()
                    ns = NameSpace()
                    ns.db_err = False
                    ns.io_err = False
                    ns.otherr = False

                    e = None

                    try:
                        #print "connecting to DB: "+SQL_BASE
                        con = lite.connect(db_path+SQL_BASE+'.db', isolation_level="IMMEDIATE")

                        with con:
                            cur = con.cursor()
                            if ('^' in SQL_CMD):
                                i = 0
                                SQL_CMDS=SQL_CMD.split('^')
                                for sql_cmd in SQL_CMDS:
                                    i+=1
                                    if (sql_cmd != ""):
                                        logger.info("DB exec #" +str(i) +": "+ sql_cmd)
                                        cur.execute(sql_cmd)
                                        con.commit()
                                    else:
                                        pass
                            else:
                                logger.info("DB exec single: "+ SQL_CMD)
                                cur.execute(SQL_CMD)
                                con.commit()

                            if ('SELECT' in SQL_CMD):
                                sql_data = cur.fetchone()
                                while (sql_data):
                                    self.socket.send ("DB_LINE: " + str(sql_data)+"\n")
                                    #BUG: does not send anything: self.socket.send ("DB_LINE: " + str(sql_data.encode("utf-8"))+"\n")
                                    sql_data = cur.fetchone()

                    except lite.Error, e:
                        ns.db_err = True
                        logger.error("DB error: %s:" % e.args[0])
                        self.socket.send ("Linbedded-SQL-SERVER: DB response: %s ; DB_ERROR" % e.args[0]+".\n")

                    except IOError as e:
                        ns.io_err = True
                        logger.error("DB error: I/O error({%s}): {%s}" % e.errno, e.strerror +"\n")
                        self.socket.send ("Linbedded-SQL-SERVER: I/O error({%s}): {%s} ; DB_ERROR" % e.errno, e.strerror +"\n")

                    #except:
                    #   ns.otherr = True
                    #   print "Unexpected error:", sys.exc_info()[0]
                    #   logger.error("DB error: Unexpected error: %s", str(sys.exc_info()[0]))
                    #   self.socket.send ("DB_ERROR: Unexpected error: %s", sys.exc_info()[0])
                    #   raise

                    finally:
                            mutex.release()

                        #print "DB STAT: "+str(ns.db_err)+"\n"
                if((len(data) > 1) and (False==ns.db_err) and (False==ns.io_err) and (False==ns.otherr)):
                    self.socket.send ("DB_OK\n")
                else:
                    pass

            elif (db_backend == 'MSSQL'):

                logger.debug("DBG: MSSQL backend code start")

                # socket text input in 'data' variable

                logger.debug("DBG: MSSQL/ODBC connect()")

                e = None
                cnxn = None
                msg = "DBG: MSSQL/ODBC connection failed: "

                try:
                    cnxn = pyodbc.connect(DSN_con_string, autocommit=True)


                except pyodbc.DatabaseError, e:
                    logger.error( str(msg) + "(DatabaseError)", exc_info=True)
                    pass

                except pyodbc.DataError, e:
                    logger.error( str(msg) + "(DataError)", exc_info=True)
                    pass

                except pyodbc.OperationalError, e:
                    logger.error( str(msg) + "(OperationalError)")
                    pass

                except pyodbc.IntegrityError, e:
                    logger.error( str(msg) + "(IntegrityError)")
                    pass

                except pyodbc.InternalError, e:
                    logger.error( str(msg) + "(InternalError)")
                    pass

                except pyodbc.ProgrammingError, e:
                    logger.error( str(msg) + "(ProgrammingError)")
                    pass

                except pyodbc.NotSupportedError, e:
                    logger.error( str(msg) + "(NotSupportedError)")
                    pass

                except pyodbc.Error, e:
                    logger.error( str(msg) + ("ODBC generic error"))
                    pass

                if (cnxn == None):
                    logger.error("MSSQL backend connection error, terminating: " % e.args[0]+".\n")
                    self.socket.send ("Linbedded-SQL-SERVER: DB error; DB_ERROR" % e.args[0]+".\n")
                    exit(0)


                logger.debug ("DBG: creation of cursor")

                cursor = cnxn.cursor()

                logger.debug("DBG: SQL input parser / regexp search")

                m=""
                CALL=""
                FLAG=""
                try:
                    m = re.search ("(.*)\$(.*)", str(data))

                    CALL=m.group(1)
                    FLAG=m.group(2)

                except Exception, e:
                    print "re.search open error:" + str(e)


                logger.debug("DBG: parsed SQL CALL: " + CALL);
                logger.debug ("DBG: parsed SQL FLAG: " + FLAG);

                # Addtional debug
                # prepare params
                #param_list=[]
                #for (param) in re.findall ("(\w+)\^", str(PARAMS)):
                #    logger.debug("p: " + param)
                #    param_list.append(param)

                #    prep_params += str(param)
                #    prep_params += ', '

                # chop last coma off
                #prep_params = prep_params.rstrip(', ')

                #logger.debug ("DBG prep_params = >" + prep_params + "<")

                logger.debug("DBG: SQL statement execute")

                em = None

                try:
                    # exec
                    cursor.execute("exec mssql_smart_weigh @IdSortownia=0, @IdLot=2, @IdZp=1, @IdProdukt=1, @MasaAktualna=5, @MasaPoprzednia=4, @IdPaleta=5, @IdStanowisko=2")

                except pyodbc.DatabaseError, e:
                    em = str(msg) + "(DatabaseError: %s)" % e
                    pass

                except pyodbc.DataError, e:
                    em = str(msg) + "(DataError: %s)" % e
                    pass

                except pyodbc.OperationalError, e:
                    em = str(msg) + "(OperationalError: %s)" % e
                    pass

                except pyodbc.IntegrityError, e:
                    em = str(msg) + "(IntegrityError: %s)" % e
                    pass

                except pyodbc.InternalError, e:
                    em = str(msg) + "(InternalError: %s)" % e
                    pass

                except pyodbc.ProgrammingError, e:
                    em = str(msg) + "(ProgrammingError: %s)" % e
                    pass

                except pyodbc.NotSupportedError, e:
                    em = str(msg) + "(NotSupportedError: %s)" % e
                    pass

                except pyodbc.Error, e:
                    em = str(msg) + "(ODBC error: %s)" % e
                    pass

                if (em != None):
                    logger.error( em, exc_info=True)
                    self.socket.send ("Linbedded-SQL-SERVER: DB response: %s ; DB_ERROR" % em + ".\n")
                    exit(0)


                if (FLAG == 'NO_DATA'):
                    # return ok
                    self.socket.send("DB_OK\n")
                    logger.debug("DBG: Sent DB_OK to Client id=" + str(self.client_id))
                    exit(0)

                logger.debug("DBG: SQL wait for fetch-all/one()")

                while 1:
                    try:
                        cursor.nextset()
                        cursor.nextset()
                        cursor.nextset()
                        cursor.nextset()
                        row = cursor.fetchall()
                    except pyodbc.Error, e:
                        logger.debug("FETCH ERROR ODBC error: %s") % e


                    logger.debug("DBG LOOP")
                    if not row:
                        break

                    if (row != None):

                        logger.debug ("DBG: DB RESPONSE RAW:  " + str(row))

                        line = str(row).translate(None, "()")
                        G_line = re.sub('Decimal', '', line)
                        logger.debug ("DBG: DB RESPONSE LINE:  " + str(G_line))

                        # clear output data global
                        out_data=""

                        # process row and convert to utf8
                        for (element) in row:
                            try:
                                logger.debug("DBG: DB-data:" + str(element))
                                element_decode="-decode-error-"
                                #if type(element) is types.StringTypes:
                                if(isinstance(element, str)):
                                    element_decode = element.decode('cp1250').encode('utf8')

                                if (element_decode == "-decode-error -"):
                                    element_decode = str(element)

                            # use default encoding to convert via str
                            #element_decode = str(element)

                                logger.debug("DBG: DB-decoded:" + str(element_decode))
                                out_data += "'" + str(element_decode) + "',"

                            ###    G_elements.append(str(element))

                            except Exception, e:
                                logger.error("ERROR: row element access error", exc_info=True)

                        # delete trailing coma
                        string=out_data
                        out_data=string[:-1]

                        #logger.debug("DBG-decoded:" + str(elements))


                    else:
                        logger.debug ("DBG: DB PROCESSING FAILED: raw row == None")

                logger.debug("DBG: closing database connection")

                cnxn.close()

                logger.debug ("out_data: " + str(out_data))

                self.socket.send (str(out_data) + "\n")

                # return ok
                self.socket.send("DB_OK\n")

                logger.debug("DBG: Sent DB_OK to Client id=" + str(self.client_id))

            else:
                logger.error("ERROR: INTERNAL CONFIG ERROR: UNSUPPORTED DB BACKEND: " + db_backend)
                exit(0)

        # finish processing
        self.socket.close();
        #print "Client id="+str(self.client_id)+" disconnected...
        logger.info ("Client id=" + str(self.client_id) + " disconnected...")


if __name__ == '__main__':

    msgbox = ctypes.windll.user32.MessageBoxA

    #str (datetime.now())
    msg="OK! Poprawnie uruchomiono serwer jako proces o PID #" + str(os.getpid()) + "\n\nW razie potrzeby zamknij proces Linbedded-SQL-Server.exe przez Process Manager'a\n" + "Windows lub zamknij system (spowoduje to automatyczne zatrzymanie serwera)."
    ret = msgbox(None, msg, 'Linbedded SQL SERVER', 0)

    while run_flag:
        tcpsock.listen(4)
        #logging.info("\nListening for incoming connections...")
        (clientsock, (ip, port)) = tcpsock.accept()
        client_id+=1
        newthread = ClientThread(ip, port, clientsock, client_id)
        newthread.start()
        threads.append(newthread)

    for t in threads:
        t.join()
