#!/bin/bash
# properties = {properties}
# use lscratch for TMPDIR if allocated
if [[ -d "/lscratch/$SLURM_JOB_ID" ]]; then
    export TMPDIR="/lscratch/$SLURM_JOB_ID"
fi
{exec_job}
