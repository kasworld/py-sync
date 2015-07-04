#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2015 SeukWon Kang (kasworld@gmail.com)
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#    http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
pysync : python dir sync server/client

usage
python server dirtosync
python client dirtosync

"""
Version = '1.3.1'

import os
import io
import sys
import math
import datetime
import random
import re
import pprint
import os.path
import zlib
import pprint
import logging
import struct
import sqlite3
import hashlib
import time
import signal
import argparse
import SocketServer
import socket
try:
    import simplejson as json
except:
    import json
import cStringIO as StringIO
import cPickle as pickle
import threading

#~ def handler(signum, frame):
#~ print 'Signal handler called with signal', signum
#~ signal.signal(signal.SIGTERM, handler)
#~ signal.signal(signal.SIGHUP, handler)
#~ signal.signal(signal.SIGINT, handler)

# sync util


def bs2s(s):  # windows \ to unix /
    return '/'.join(s.split('\\'))


def ospathjoin(b, s):
    return bs2s(os.path.join(b, s))


def filesystemencoding():
    code = None
    fn = lambda x: x

    def decode(s):
        if type(s) == str:
            return s.decode(fn(code))
        elif type(s) == unicode:
            return s
        else:
            raise Exception('unknown type %s' % type(s))

    def encode(s):
        if type(s) == str:
            return s
        elif type(s) == unicode:
            return s.encode(code)
        else:
            raise Exception('unknown type %s' % type(s))
    platformstr = sys.platform
    if platformstr.startswith('linux'):
        code = 'utf8'
    elif platformstr == 'win32':
        code = 'euckr'
        fn = bs2s
    else:
        raise Exception('unknown OS')
    return encode, decode

toFSstr, fromFSstr = filesystemencoding()


class DictAsProperty(dict):
    validFields = {
    }

    def __init__(self, data=None):
        if data:
            dict.__setattr__(self, 'validFields', data.copy())
            dict.__init__(self, data)
        else:
            dict.__init__(self, self.validFields)

    def __getitem__(self, name):
        if name in self.validFields:
            return dict.__getitem__(self, name)
        raise AttributeError(name)

    def __getattr__(self, name):
        if name in self.validFields:
            return dict.__getitem__(self, name)
        raise AttributeError(name)

    def __setitem__(self, name, value):
        if name in self.validFields:
            dict.__setitem__(self, name, value)
            return self
        raise AttributeError(name)

    def __setattr__(self, name, value):
        if name in self.validFields:
            dict.__setitem__(self, name, value)
            return self
        raise AttributeError(name)


def makeDir(dirpath):
    # dirpath 가 생기도록 ( 그 위의 폴더까지 포함해서 ) 만들어줌.
    # 이미 있거나 권한이 없으면 exception
    rstr, ertn = '', 0
    try:
        os.makedirs(dirpath)
    except OSError as e:
        rstr = str(e)
        ertn = -1
    return rstr, ertn


def loadLocalFile(fname, basedir):
    fullname = ospathjoin(basedir, fname)
    if not os.path.exists(fullname):
        return None
    try:
        with open(fullname, 'rb') as f:
            body = f.read()
    except Exception as e:
        print str(e)
        body = None
    return body


def saveLocalFile(fname, basedir, body):
    fullname = ospathjoin(basedir, fname)

    # makedir to create file
    dirname, filename = os.path.split(fullname)
    makeDir(dirname)
    try:
        if os.path.exists(fullname):
            os.rename(fullname, fullname + '.tmp')
        f = open(fullname, 'w+b')
        if type(body) == list:
            f.writelines(body)
        else:
            f.write(body)
        f.close()
        if os.path.exists(fullname + '.tmp'):
            os.remove(fullname + '.tmp')
        return True
    except OSError as e:
        print 'save fail', str(e), fullname
        return False


def getFileCrc32(fname, basedir):
    fullname = ospathjoin(basedir, fname)
    if not os.path.exists(fullname):
        return -1
    rtn = 0
    try:
        with open(fullname, 'rb') as f:
            while True:
                body = f.read(0x100000)
                if not body:
                    break
                rtn = zlib.crc32(body, rtn)
    except Exception as e:
        print str(e)
        return -1
    return rtn & 0xffffffff


def getFileInfo(fname, basedir):
    fullname = ospathjoin(basedir, fname)
    fcrc = getFileCrc32(fname, basedir)
    if fcrc == -1:
        return fname, []
    return fname, [os.path.getmtime(fullname), os.path.getsize(fullname), fcrc]


def getFileList(basedir):
    basedir = fromFSstr(os.path.abspath(basedir))
    rtn = {}
    for root, dirs, files in os.walk(basedir):
        for name in files:
            fname = fromFSstr(ospathjoin(root, name))
            fname = fname[len(basedir) + 1:]
            k, v = getFileInfo(fname, basedir)
            if not v:
                return None
            rtn[k] = v

    return rtn


def serialDumps(objs):
    # return json.dumps( objs , ensure_ascii = False,  separators=(',',':') )
    return pickle.dumps(objs,  pickle.HIGHEST_PROTOCOL)


def serialLoads(objs):
    return pickle.loads(objs)
    # return json.loads( objs )


def getStatFilename(basedir, addedinfo):
    return "%s.%s" % (hashlib.md5(basedir + addedinfo).hexdigest(), sys.argv[1])


def saveSyncedFileList(syncedlist, basedir, addedinfo):
    #listname = "%s.%s" % ( hashlib.md5( basedir + addedinfo ).hexdigest()  , sys.argv[1]  )
    listname = getStatFilename(basedir, addedinfo)
    strs = serialDumps(syncedlist)
    progbase = os.path.dirname(os.path.abspath(sys.argv[0]))
    saveLocalFile(listname, progbase, strs)


def loadSyncedFileList(basedir, addedinfo):
    #listname = hashlib.md5( basedir + addedinfo ).hexdigest()
    listname = getStatFilename(basedir, addedinfo)
    progbase = os.path.dirname(os.path.abspath(sys.argv[0]))
    strs = loadLocalFile(listname, progbase)
    if strs == None:
        return {}
    try:
        rtn = serialLoads(strs)
    except Exception as e:
        print 'load synced filelist fail', str(e)
        rtn = {}
    return rtn


def diffFileList(locallist, remotelist):
    lset = set(locallist.keys())
    rset = set(remotelist.keys())

    comset = lset & rset

    localupdateset = rset - comset
    localupdatelist = {}
    for k in localupdateset:
        localupdatelist[k] = remotelist[k]

    remoteupdateset = lset - comset
    remoteupdatelist = {}
    for k in remoteupdateset:
        remoteupdatelist[k] = locallist[k]

    for k in comset:
        if locallist[k][1:] != remotelist[k][1:]:
            if locallist[k][0] > remotelist[k][0]:
                remoteupdatelist[k] = locallist[k]
            elif locallist[k][0] < remotelist[k][0]:
                localupdatelist[k] = remotelist[k]
            else:
                print 'conflict', k, locallist[k], remotelist[k]

    return localupdatelist, remoteupdatelist


def getDeletedList(oldlist, newlist):
    oldset = set(oldlist.keys())
    newset = set(newlist.keys())
    delset = oldset - newset
    dellist = {}
    for k in delset:
        dellist[k] = oldlist[k]
    return dellist


def removeListFrom(srclist, dstlist):
    srcset = set(srclist.keys())
    dstset = set(dstlist.keys())
    interset = srcset & dstset
    for k in interset:
        del srclist[k]


class FileList(dict):

    def __init__(self, data={}):
        dict.__init__(self, data)

    def load(self, basedir):
        pass

    def save(self, filename):
        pass

    def union(self, target):
        pass

    def interaction(self, target):
        pass

    def difference(self, target):
        pass

    def symmetric_difference(self, target):
        pass

# logging


def getLogger(level=logging.DEBUG):
    # create logger
    logger = logging.getLogger('noname')
    logger.setLevel(level)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(level)
    # create formatter
    formatter = logging.Formatter("%(asctime)s:%(levelno)s: %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    return logger

g_logger = getLogger()


def getServerSocket(HOST, PORT):
    s = None
    print 'wait client connect'
    for res in socket.getaddrinfo(HOST, PORT, socket.AF_UNSPEC,
                                  socket.SOCK_STREAM, 0, socket.AI_PASSIVE):
        af, socktype, proto, canonname, sa = res
        try:
            s = socket.socket(af, socktype, proto)
        except socket.error, msg:
            print 'socket', msg
            s = None
            continue
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(sa)
            s.listen(1)
        except socket.error, msg:
            print 'bind', msg
            s.close()
            s = None
            continue
        break
    if s is None:
        print 'could not open socket'
        return None, None
        # sys.exit(1)
    return s.accept()


def getClientSocket(HOST, PORT):
    s = None
    for res in socket.getaddrinfo(HOST, PORT, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, canonname, sa = res
        try:
            s = socket.socket(af, socktype, proto)
        except socket.error, msg:
            s = None
            continue
        try:
            s.connect(sa)
        except socket.error, msg:
            s.close()
            s = None
            continue
        break
    if s is None:
        print 'could not open socket'
        sys.exit(1)
    return s


class SocketThread(object):
    headerFormat = "!Q"
    headerLen = struct.calcsize(headerFormat)
    headerStruct = struct.Struct(headerFormat)

    def __init__(self, basedir):
        self.session = DictAsProperty({
            'conninfo': (),
            'socket': None,
            'sendQ': [],
            'remainChunk': '',
            'totalsent': 0,
            'zipsent': 0,
            'totalrecv': 0,
            'ziprecv': 0,
            'basedir': basedir,
        })

    def recvChunkTill(self, lentorecv):
        totallen = len(self.session.remainChunk)
        chunklist = [self.session.remainChunk]
        torecv = lentorecv - totallen
        while torecv > 0:
            if not self.session.socket:
                return []
            rdata = self.session.socket.recv(0x10000)
            if not rdata:
                return []
            chunklist.append(rdata)
            torecv -= len(rdata)

        rtn = []
        rtnsize = 0
        pushback = []
        while rtnsize < lentorecv:
            r = chunklist.pop(0)
            rtn.append(r[: lentorecv - rtnsize])
            pushback = r[lentorecv - rtnsize:]
            rtnsize += len(rtn[-1])
        if pushback:
            chunklist.insert(0, pushback)
        self.session.remainChunk = ''.join(chunklist)
        return rtn

    def recvI64packet(self):
        header = self.recvChunkTill(SocketThread.headerLen)
        if not header:
            return []
        bodylen,  = SocketThread.headerStruct.unpack(''.join(header))
        if bodylen == 0:
            return []
        body = self.recvChunkTill(bodylen)
        #~ if not body :
        #~ return []
        return body

    def recvData(self):
        recvPacket = None
        try:
            recvPacket = self.recvI64packet()
            # print 'recvData' , recvPacket
        except socket.timeout as e:
            g_logger.error('recv timeout')
            return recvPacket
        except socket.error as msg:
            # bad file descriptor, broken pipe, connection reset by peer
            if msg.errno not in [9, 32, 104, socket.errno.ETIMEDOUT]:
                g_logger.error('recv %s,%s' % (msg.errno, msg))
            return recvPacket
        except Exception as e:
            g_logger.error('recv %s' % e)
            return recvPacket
        if not recvPacket:
            g_logger.error('recv socket invalid')
            return recvPacket

        unzipper = zlib.decompressobj()
        unzipchunk = []
        zipsize = 0
        unzipsize = 0
        for rp in recvPacket:
            unziped = unzipper.decompress(rp)
            # print 'unziped' , type(unziped), unziped
            unzipchunk.append(unziped)
            zipsize += len(rp)
            unzipsize += len(unziped)
        unziped = unzipper.flush()
        unzipchunk.append(unziped)
        unzipsize += len(unziped)

        self.session.ziprecv += zipsize
        self.session.totalrecv += unzipsize

        return unzipchunk
        # return ''.join( unzipchunk )

    def checkBody(self, datalist, cklist):
        crc = 0
        size = 0
        for d in datalist:
            crc = zlib.crc32(d, crc)
            size += len(d)

        if size != cklist[1]:
            g_logger.info('size mismatch, %s should %s ' % (size, cklist[1]))
            return False

        if crc & 0xffffffff != cklist[2]:
            g_logger.info('crc32 mismatch, %s should %s ' % (crc, cklist[2]))
            return False

        return True

    def recvFiles(self, filelist):
        for f in sorted(filelist.keys()):
            g_logger.info('recv %s' % f)
            toload = self.recvData()
            if self.checkBody(toload, filelist[f]):
                saveLocalFile(f, self.session.basedir, toload)
            else:
                self.errCleanUp('recv checkBody fail, %s' % f)
                return False
        return True

    def sendQueue(self, size, queue):
        header = SocketThread.headerStruct.pack(size)
        self.session.sendQ.append(header)
        if size > 0:
            self.session.sendQ.extend(queue)
        if not self.session.socket:
            return
        while self.session.sendQ:
            try:
                self.session.socket.sendall(self.session.sendQ.pop(0))
            except socket.timeout as e:
                g_logger.error('send timeout')
                break
            except socket.error as msg:
                # bad file descriptor, broken pipe, connection reset by peer
                if msg.errno not in [9, 32, 104]:
                    g_logger.error('send %s,%s' % (msg.errno, msg))
                break
            except Exception as e:
                g_logger.error('unknown send %s' % e)
                break
        return

    def sendData(self, data):
        self.session.totalsent += len(data)
        data = zlib.compress(data)
        self.session.zipsent += len(data)
        self.sendQueue(len(data), [data])

    def sendObject(self, d):
        self.sendData(serialDumps(d))

    def checkCrcZipSendFile(self, fname, cklist):
        # open
        # crc calc compress
        fullname = ospathjoin(self.session.basedir, fname)
        if not os.path.exists(fullname):
            return False
        if os.path.getsize(fullname) != cklist[1]:
            g_logger.info('size mismatch, %s should %s ' %
                          (os.path.getsize(fullname), cklist[1]))
            return False

        zipper = zlib.compressobj()
        zipchunk = []
        zippedsize = 0
        crc = 0
        try:
            with open(fullname, 'rb') as f:
                while True:
                    body = f.read(0x100000)
                    if not body:
                        break
                    crc = zlib.crc32(body, crc)
                    zipped = zipper.compress(body)
                    zippedsize += len(zipped)
                    zipchunk.append(zipped)
            zipped = zipper.flush()
            zippedsize += len(zipped)
            zipchunk.append(zipped)

        except Exception as e:
            print str(e)
            return False
        if crc & 0xffffffff != cklist[2]:
            g_logger.info('crc32 mismatch, %s should %s ' % (crc, cklist[2]))
            return False

        # send
        self.session.totalsent += cklist[1]
        self.session.zipsent += zippedsize
        self.sendQueue(zippedsize, zipchunk)
        return True

    def sendFiles(self, filelist):
        for f in sorted(filelist.keys()):
            g_logger.info('sending %s' % f)
            if not self.checkCrcZipSendFile(f, filelist[f]):
                return False
        return True

    def removeFiles(self, filelist):
        for f in filelist:
            fullname = ospathjoin(self.session.basedir, f)
            if os.path.exists(fullname):
                g_logger.info('delete %s' % f)
                os.remove(fullname)

    def server(self, conninfo):
        self.session.socket, self.session.conninfo = getServerSocket(*conninfo)
        if not self.session.socket:
            return
        g_logger.info('connected from %s' % str(self.session.conninfo))

        g_logger.info('receiving filelist')
        try:
            clientfilelist, clientdeletedlist = serialLoads(
                ''.join(self.recvData()))
        except Exception as e:
            print str(e)
            self.errCleanUp('fail to recv/read filelist')
            return

        g_logger.info('load filelist')
        syncedfilelist = loadSyncedFileList(
            self.session.basedir, self.session.conninfo[0])
        serverfilelist = getFileList(self.session.basedir)
        if serverfilelist == None:
            self.errCleanUp('fail to read file list')
            return

        serverdeletedlist = getDeletedList(syncedfilelist, serverfilelist)

        serverupdatelist, clientupdatelist = diffFileList(
            serverfilelist, clientfilelist)

        removeListFrom(serverupdatelist, serverdeletedlist)
        removeListFrom(clientupdatelist, clientdeletedlist)

        g_logger.info('send filelist')
        self.sendObject(
            [serverupdatelist, clientupdatelist, serverdeletedlist])

        g_logger.info('delete files')
        self.removeFiles(clientdeletedlist)

        g_logger.info('recv files')
        if not self.recvFiles(serverupdatelist):
            return

        g_logger.info('send files')
        if not self.sendFiles(clientupdatelist):
            return

        g_logger.info('save filelist')
        removeListFrom(serverfilelist, clientdeletedlist)
        serverfilelist.update(serverupdatelist)

        # must getFileList( self.session.basedir ) == serverfilelist
        #~ cklist = getFileList( self.session.basedir )
        #~ print diffFileList( cklist, serverfilelist)

        saveSyncedFileList(
            serverfilelist, self.session.basedir, self.session.conninfo[0])

        g_logger.info('clean up')
        self.cleanUp()

    def client(self, conninfo):
        self.session.socket, self.session.conninfo = getClientSocket(
            *conninfo), conninfo
        g_logger.info('client: connected %s' % str(self.session.conninfo))

        g_logger.info('load filelist')
        clientfilelist = getFileList(self.session.basedir)
        if clientfilelist == None:
            self.errCleanUp('fail to read file list')
            return
        syncedfilelist = loadSyncedFileList(
            self.session.basedir, self.session.conninfo[0])
        deletedfilelist = getDeletedList(syncedfilelist, clientfilelist)

        g_logger.info('send filelist')
        self.sendObject([clientfilelist, deletedfilelist])

        g_logger.info('receiving filelist')
        try:
            serverupdatelist, clientupdatelist, serverdeletedlist = serialLoads(
                ''.join(self.recvData()))
        except Exception as e:
            print str(e)
            self.errCleanUp('fail to recv/read filelist')
            return

        g_logger.info('delete files')
        self.removeFiles(serverdeletedlist)

        g_logger.info('send files')
        if not self.sendFiles(serverupdatelist):
            return

        g_logger.info('recv files')
        if not self.recvFiles(clientupdatelist):
            return

        g_logger.info('save filelist')
        removeListFrom(clientfilelist, serverdeletedlist)
        clientfilelist.update(clientupdatelist)

        # must getFileList( self.session.basedir ) == serverfilelist
        #~ cklist = getFileList( self.session.basedir )
        #~ print diffFileList( cklist, clientfilelist)

        saveSyncedFileList(
            clientfilelist, self.session.basedir, self.session.conninfo[0])

        g_logger.info('clean up')
        self.cleanUp()

    def cleanUp(self):
        self.session.socket.close()
        g_logger.info(
            'recv {totalrecv}:{ziprecv} , sent {totalsent}:{zipsent}'.format(**self.session))

    def errCleanUp(self, msg):
        self.session.socket.close()
        g_logger.info('Error exit %s' % msg)


def testme():
    #pprint.pprint( json.dumps( getFileList( '.' ) , separators=(',',':') ) )
    #~ pprint.pprint(
        #~ diffFileList( getFileList( 'test1' ) , getFileList( 'test2' ) )
        #~ )
    #~ a = jsonPacket().toJson()
    #~ b = jsonPacket().fromJson( a )
    #~ print a, b
    a = getFileList('/home/kasw/Videos')
    ss = a.keys()[0]
    print ss, type(ss), fromFSstr(ss), type(fromFSstr(ss))
    print a


if __name__ == "__main__":
    print 'pysync version:', Version
    if len(sys.argv) > 2 and sys.argv[1] == 'server':
        basedir = sys.argv[2]
        conninfo = None, 8088
        while 1:
            server = SocketThread(basedir)
            server.server(conninfo)
            time.sleep(1)
    elif len(sys.argv) > 2 and sys.argv[1] == 'client':
        #conninfo = '10.1.4.233' , 8088
        #conninfo = '192.168.0.198' , 8088
        conninfo = 'localhost', 8088
        basedir = sys.argv[2]
        client = SocketThread(basedir)
        client.client(conninfo)
    else:
        testme()
