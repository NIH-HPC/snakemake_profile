"""
Biowulf helper functions for snakemake workflows.
Include this file at the top of your Snakefile:

    import os
    include: os.path.expanduser("~/.config/snakemake/biowulf/biowulf.smk")
"""

def tmpdir(wildcards, resources):
    """Use lscratch for TMPDIR if allocated, otherwise fall back to /tmp.
    
    Use this as the tmpdir resource in any rule that requests lscratch:

        rule example:
            resources:
                disk_mb=51200,
                slurm_extra="'--gres=lscratch:50'",
                tmpdir=tmpdir
    """
    if resources.disk_mb > 0:
        return "/lscratch/$SLURM_JOB_ID"
    return "/tmp"
