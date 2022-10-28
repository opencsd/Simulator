#!/bin/bash

cd /root/workspace/KETI-Pushdown-Process-Container/build && make -j 40

cp /root/workspace/KETI-Pushdown-Process-Container/build/src/csdWorkerModule/CSDInstance /root/workspace/KETI-Pushdown-Process-Container/dockerbuild/csdWorkerModule/_output
cp /root/workspace/KETI-Pushdown-Process-Container/asset/GeneratedCSDTableManager.json /root/workspace/KETI-Pushdown-Process-Container/dockerbuild/csdWorkerModule/

cp /root/workspace/KETI-Pushdown-Process-Container/build/src/storageEngineInstance/EngineInstance /root/workspace/KETI-Pushdown-Process-Container/dockerbuild/storageEngineInstance/_output
cp /root/workspace/KETI-Pushdown-Process-Container/asset/GeneratedCSDTableManager.json /root/workspace/KETI-Pushdown-Process-Container/dockerbuild/storageEngineInstance/

cd /root/workspace/KETI-Pushdown-Process-Container/dockerbuild/csdWorkerModule && docker build -t ketidevit2/csd-docker:latest /root/workspace/KETI-Pushdown-Process-Container/dockerbuild/csdWorkerModule

cd /root/workspace/KETI-Pushdown-Process-Container/dockerbuild/storageEngineInstance && docker build -t ketidevit2/se-docker:latest /root/workspace/KETI-Pushdown-Process-Container/dockerbuild/storageEngineInstance

docker push ketidevit2/csd-docker:latest
docker push ketidevit2/se-docker:latest