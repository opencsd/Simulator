#!/bin/bash
docker run -d --name csd1 -e CSDNAME=csd1 -e CSDNUM=1 -v /data:/data -v /data/csd1.sock:/data/csd1.sock -p 18080:18080 -p 28080:28080 ketidevit2/csd-docker:latest 
docker run -d --name csd2 -e CSDNAME=csd2 -e CSDNUM=2 -v /data:/data -v /data/csd2.sock:/data/csd2.sock -p 18081:18080 -p 28081:28080 ketidevit2/csd-docker:latest
docker run -d --name csd3 -e CSDNAME=csd3 -e CSDNUM=3 -v /data:/data -v /data/csd3.sock:/data/csd3.sock -p 18082:18080 -p 28082:28080 ketidevit2/csd-docker:latest
docker run -d --name csd4 -e CSDNAME=csd4 -e CSDNUM=4 -v /data:/data -v /data/csd4.sock:/data/csd4.sock -p 18083:18080 -p 28083:28080 ketidevit2/csd-docker:latest
docker run -d --name csd5 -e CSDNAME=csd5 -e CSDNUM=5 -v /data:/data -v /data/csd5.sock:/data/csd5.sock -p 18084:18080 -p 28084:28080 ketidevit2/csd-docker:latest
docker run -d --name csd6 -e CSDNAME=csd6 -e CSDNUM=6 -v /data:/data -v /data/csd6.sock:/data/csd6.sock -p 18085:18080 -p 28085:28080 ketidevit2/csd-docker:latest
docker run -d --name csd7 -e CSDNAME=csd7 -e CSDNUM=7 -v /data:/data -v /data/csd7.sock:/data/csd7.sock -p 18086:18080 -p 28086:28080 ketidevit2/csd-docker:latest
docker run -d --name csd8 -e CSDNAME=csd8 -e CSDNUM=8 -v /data:/data -v /data/csd8.sock:/data/csd8.sock -p 18087:18080 -p 28087:28080 ketidevit2/csd-docker:latest
