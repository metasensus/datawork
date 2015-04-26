#!/usr/bin/env python
#!/usr/bin/env python
import string;
import os;
folder_array=['mpathb','mpathc','mpathd','mpathe','mpathf','mpathg','mpathh','mpathi','mpathj','mpathk','mpathl','mpathm','mpathn','mpatho','mpathp','mpathq','mpathr','mpathr','mpaths','mpatht','mpathu'];

fl=open('create_partitions.sh','w');
grantfl=open('grant_permissions.sh','w');
ctr=1;
mountarray=[];
for folder in folder_array:
    fl.write('echo fdisk\n');
    fl.write('fdisk /dev/mapper/'+folder+'\n');
    fl.write('echo mkfs.ext4\n');
    fl.write('mkfs.ext4 /dev/mapper/'+folder+'\n');
    fl.write('echo vertica\n');
    if len(str(ctr))==1:
        foldername='0'+str(ctr);
    else:
        foldername=str(ctr);
    fl.write('mkdir /vertica/data'+foldername+'\n');
    grantfl.write('chown -R dbadmin:dbadmin /vertica/data'+foldername+'\n');
    
    mountarray.append('echo /dev/mapper/'+folder+'\t'+'/vertica/data'+foldername+'\t\text4\tdefaults\t\t\t1 2');
    ctr=ctr+1;
fl.write(string.join(mountarray,'\n'));
fl.close();
grantfl.close();
os.system('chmod +x create_partitions.sh');
os.system('chmod +x grant_permissions.sh');
