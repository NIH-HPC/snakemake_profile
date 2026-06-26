"""
Test workflow for the biowulf snakemake9 profile.
Run from this directory with:

    mkdir -p tests logs
    snakemake --profile biowulf

Each rule tests a different resource combination to verify
that the profile submits jobs correctly to Biowulf.
"""

rule all:
    input:
        "tests/norm",
        "tests/force_norm",
        "tests/quick",
        "tests/gpu",
        "tests/gpu2",
        "tests/scratch",
        "tests/ntasks",

rule clean:
    shell:
        "rm -f tests/* logs/*"

rule norm:
    """Normal job: threads + mem + runtime only. Should land on norm partition."""
    output: "tests/norm"
    threads: 10
    resources:
        runtime=600,
        mem_mb=1024
    shell:
        "echo $TMPDIR; touch {output}"

rule force_norm:
    """Explicitly request norm partition."""
    output: "tests/force_norm"
    threads: 10
    resources:
        runtime=10,
        mem_mb=1024,
        slurm_partition="norm"
    shell:
        "touch {output}"

rule quick:
    """Job with lscratch. Should land on norm (quick partition needs < 4h, etc.)"""
    output: "tests/quick"
    threads: 4
    resources:
        runtime=10,
        mem_mb=1024,
        disk_mb=10240,
        slurm_extra="'--gres=lscratch:10'"
    shell:
        "echo $TMPDIR; touch {output}"

rule gpu:
    """GPU job with a specific model. Should land on gpu partition."""
    output: "tests/gpu"
    threads: 8
    resources:
        runtime=60,
        mem_mb=16384,
        slurm_partition="gpu",
        gpu=1,
        gpu_model="a100"
    shell:
        "touch {output}"

rule gpu2:
    """GPU job using constraint-style selection (multiple acceptable models)."""
    output: "tests/gpu2"
    threads: 8
    resources:
        runtime=60,
        mem_mb=16384,
        slurm_partition="gpu",
        gpu=2,
        slurm_extra="'--constraint=[gpua100|gpuv100x]'"
    shell:
        "touch {output}"

rule scratch:
    """Job requesting lscratch. Verifies $TMPDIR is set correctly."""
    output: "tests/scratch"
    threads: 4
    resources:
        runtime=30,
        mem_mb=4096,
        disk_mb=51200,
        slurm_extra="'--gres=lscratch:50'"
    shell:
        "echo $TMPDIR; touch {output}"

rule ntasks:
    """Multi-task job. Should land on multinode partition."""
    output: "tests/ntasks"
    threads: 1
    resources:
        runtime=60,
        mem_mb=4096,
        slurm_partition="multinode",
        slurm_extra="'--ntasks=16 --nodes=2'"
    shell:
        "touch {output}"
