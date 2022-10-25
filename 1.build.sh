#!/bin/bash

cd /root/workspace/KETI-Pushdown-Process-Container/build && make -j 40

cp /root/workspace/KETI-Pushdown-Process-Container/build/src/csdWorkerModule/CSDInstance /root/workspace/KETI-Pushdown-Process-Container/dockerbuild/_output
cp /root/workspace/KETI-Pushdown-Process-Container/asset/GeneratedCSDTableManager.json /root/workspace/KETI-Pushdown-Process-Container/dockerbuild

cd /root/workspace/KETI-Pushdown-Process-Container/dockerbuild && docker build -t ketidevit2/csd-docker:latest /root/workspace/KETI-Pushdown-Process-Container/dockerbuild

docker push ketidevit2/csd-docker:latest