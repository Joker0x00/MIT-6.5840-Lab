#!/bin/bash
python3 runtests.py TestRejoin3B -n 200 -o ./res -p 16
# runtests.py TestSnapshotBasic3D TestSnapshotInstall3D TestSnapshotInstallUnreliable3D TestSnapshotInstallCrash3D TestSnapshotInstallUnCrash3D TestSnapshotAllCrash3D TestSnapshotInit3D