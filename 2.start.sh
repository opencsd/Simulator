#!/bin/bash
docker run -d --name csd1 -e CSDNAME=csd1 -e CSDNUM=1 -v /data:/data -v /root/data/csd1/data.sock:/root/data/csd1/data.sock -p 18080:18080 -p 28080:28080 ketidevit2/csd-docker:latest 
docker run -d --name csd2 -e CSDNAME=csd2 -e CSDNUM=2 -v /data:/data -v /root/data/csd2/data.sock:/root/data/csd2/data.sock -p 18081:18080 -p 28081:28080 ketidevit2/csd-docker:latest
docker run -d --name csd3 -e CSDNAME=csd3 -e CSDNUM=3 -v /data:/data -v /root/data/csd3/data.sock:/root/data/csd3/data.sock -p 18082:18080 -p 28082:28080 ketidevit2/csd-docker:latest
docker run -d --name csd4 -e CSDNAME=csd4 -e CSDNUM=4 -v /data:/data -v /root/data/csd4/data.sock:/root/data/csd4/data.sock -p 18083:18080 -p 28083:28080 ketidevit2/csd-docker:latest
docker run -d --name csd5 -e CSDNAME=csd5 -e CSDNUM=5 -v /data:/data -v /root/data/csd5/data.sock:/root/data/csd5/data.sock -p 18084:18080 -p 28084:28080 ketidevit2/csd-docker:latest
docker run -d --name csd6 -e CSDNAME=csd6 -e CSDNUM=6 -v /data:/data -v /root/data/csd6/data.sock:/root/data/csd6/data.sock -p 18085:18080 -p 28085:28080 ketidevit2/csd-docker:latest
docker run -d --name csd7 -e CSDNAME=csd7 -e CSDNUM=7 -v /data:/data -v /root/data/csd7/data.sock:/root/data/csd7/data.sock -p 18086:18080 -p 28086:28080 ketidevit2/csd-docker:latest
docker run -d --name csd8 -e CSDNAME=csd8 -e CSDNUM=8 -v /data:/data -v /root/data/csd8/data.sock:/root/data/csd8/data.sock -p 18087:18080 -p 28087:28080 ketidevit2/csd-docker:latest
