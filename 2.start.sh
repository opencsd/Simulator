#!/bin/bash
docker run -d --name csd1 -e CSDNAME=csd1 -e CSDNUM=1 -v /data:/data ketidevit2/csd-docker:latest 
docker run -d --name csd2 -e CSDNAME=csd2 -e CSDNUM=2 -v /data:/data ketidevit2/csd-docker:latest
docker run -d --name csd3 -e CSDNAME=csd3 -e CSDNUM=3 -v /data:/data ketidevit2/csd-docker:latest
docker run -d --name csd4 -e CSDNAME=csd4 -e CSDNUM=4 -v /data:/data ketidevit2/csd-docker:latest
docker run -d --name csd5 -e CSDNAME=csd5 -e CSDNUM=5 -v /data:/data ketidevit2/csd-docker:latest
docker run -d --name csd6 -e CSDNAME=csd6 -e CSDNUM=6 -v /data:/data ketidevit2/csd-docker:latest
docker run -d --name csd7 -e CSDNAME=csd7 -e CSDNUM=7 -v /data:/data ketidevit2/csd-docker:latest
docker run -d --name csd8 -e CSDNAME=csd8 -e CSDNUM=8 -v /data:/data ketidevit2/csd-docker:latest
