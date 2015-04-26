#!/usr/bin/env python



import time;
import oracleconnect as oracon;
import os;
import string;
import sys;
import ssdhtml as ssd;
import nlhtml as nl;
import fchtml as fc;
import graphjson as sjson;
import lifelefthtml as lifeleft;
import lifeleftjson as ljson;
import fsbhtmlread as fsb;
import fsbjson as fjson;

while(1):
	sjson.get_data();	
	ssd.get_data();
	nl.get_data();
	fc.get_data();
	lifeleft.get_data();
	ljson.get_data();
	fsb.get_data();
	fjson.get_data();
	time.sleep(3600*8);

