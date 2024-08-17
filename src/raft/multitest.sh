#!/bin/bash
python3 runtests.py 3A 3B 3C 3D -n 500 -o ./res -p 10
# runtests.py TestSnapshotBasic3D TestSnapshotInstall3D TestSnapshotInstallUnreliable3D TestSnapshotInstallCrash3D TestSnapshotInstallUnCrash3D TestSnapshotAllCrash3D TestSnapshotInit3D