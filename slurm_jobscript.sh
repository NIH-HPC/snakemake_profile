#!/bin/bash
# properties = {properties}

# if lscratch exists use it for tempdir
if [[ -d "/lscratch/$SLURM_JOB_ID" ]] ; then
    tmp="/lscratch/$SLURM_JOB_ID/tmp"
    mkdir "$tmp"
    export TMPDIR="$tmp"
fi

{exec_job}
