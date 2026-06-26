"""
Biowulf helper functions for snakemake workflows.
Include this file at the top of your Snakefile:

    import os
    include: os.path.expanduser("~/.config/snakemake/biowulf/biowulf.smk")
"""
lscratch_tmpdir = "/lscratch/$SLURM_JOB_ID"
