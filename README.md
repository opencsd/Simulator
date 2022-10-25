## Introduction of KETI-OpenCSD Pushdown-Process-Container
-------------
![Alt text](/%EC%82%AC%EC%A7%84/pushdown.jpg)

KEIT-Pushdown-Process-Container for KETI-OpenCSD Platform

Developed by KETI

## Contents
-------------
[1. Requirement](#requirement)

[2. How To Install](#How-To-Install)

[3. Modules](#modules)

[4. Governance](#governance)

## Requirement
-------------
>   Ubuntu 20.04.2 LTS

>   MySQL 5.6(Storage Engine : MyROCSK by KETI-Version)

>   RapidJSON

## How To Install
-------------
```bash
git clone 
cd Pushdown-Process-Container
make -j8
```

## Modules
-------------
### Storage Engine Input Interface
-------------
A module that listens for tasks sent by the Handler.

Work ID is assigned to each task, and scan-related tasks can be pushdown to CSD.

### Snippet Scheduler
-------------
Snippet Scheduler Module is Sched Snippet and Pushdown to CSD

Snippet Scheduler's Scoring based on CSD working Block count

### Table Manager
-------------
Module that stores table information required for scan pushdown

### CSD Manager
-------------
Module that manages CSD information to perform Pushdown

Manage host IP to which CSD is connected, tunneling IP, CSD performance, and the number of blocks being performed by CSD

### LBA2PBA Query Agent
-------------
Module that converts File Offset to Physical Block Address

### Column Encoding
-------------
Module that converts pushdown conditional information according to the type of each column

### Buffer Manager
-------------
Module that receives and merges pushdown results with CSD

Merge based on Work ID

## Governance
-------------
This work was supported by Institute of Information & communications Technology Planning & Evaluation (IITP) grant funded by the Korea government(MSIT) (No.2021-0-00862, Development of DBMS storage engine technology to minimize massive data movement)

## Others
-------------
Due to the structure change, in the first half of the year, we worked on local repositories and private github.
> https://github.com/KETI-OpenCSD/Pushdown-Process-Container
