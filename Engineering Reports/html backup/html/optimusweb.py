#!/usr/bin/env python
import time;
import optimuslib as lib;

while(1):
    lib.read_data();
    lib.write_data();
    lib.total_data();
    time.sleep(3600);