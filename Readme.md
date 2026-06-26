# Snakemake profile for Biowulf — Snakemake ≥ 9

Minimal [Snakemake profile](https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles)
for the [NIH Biowulf](https://hpc.nih.gov) cluster using the
[snakemake-executor-plugin-slurm](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html).

> **Requires Snakemake ≥ 9 and `snakemake-executor-plugin-slurm`.**  
> For Snakemake 7, use the `main` branch of this repository.

---

## Installation

```bash
module load snakemake/9.23.1

mkdir -p ~/.config/snakemake
git clone --branch snakemake9 \
    https://github.com/NIH-HPC/snakemake_profile.git \
    ~/.config/snakemake/biowulf
```

Then run any workflow with:

```bash
snakemake --profile biowulf [other options]
```

---

## How it works

This profile sets `executor: slurm` and Biowulf-appropriate defaults in `config.yaml`.
Job submission, status polling, and cancellation are all handled internally by the
executor plugin — no custom Python scripts are needed.

All resource information is read from each rule's `threads:` and `resources:` directives.

The profile sets Biowulf-required rate limits on SLURM calls
(`max-jobs-per-second`, `max-status-checks-per-second`) as
[requested by NIH HPC staff](https://hpc.nih.gov/apps/snakemake.html).

---

## Resources

### Standard resources

| Rule directive | SLURM flag | Default |
|---|---|---|
| `threads` | `--cpus-per-task` | 1 |
| `resources: mem_mb` | `--mem` | 4096 MB |
| `resources: runtime` | `--time` | 120 min |
| `resources: slurm_partition` | `--partition` | `norm` |

### Partition selection

Unlike the old profile, **partition selection is not automatic**. Rules that need
a non-default partition must set it explicitly:

```python
resources:
    slurm_partition="gpu"       # GPU jobs
    slurm_partition="largemem"  # > 500 GB memory
    slurm_partition="multinode" # multi-node MPI jobs
    slurm_partition="quick"     # < 4 hours, < 16 cores, < 370 GB
```

The `norm` partition is used for everything else.

### lscratch (local scratch disk)

Biowulf's local scratch (`/lscratch/$SLURM_JOB_ID`) must be requested via
`--gres=lscratch:N` (N in GB). When allocated, Biowulf sets `$TMPDIR` automatically.
The executor plugin does not translate `disk_mb` to lscratch, so use `slurm_extra`:

```python
rule with_scratch:
    output: "results/output"
    threads: 8
    resources:
        mem_mb=16384,
        runtime=240,
        disk_mb=102400,                           # informational for Snakemake scheduling
        slurm_extra="'--gres=lscratch:100'",      # 100 GB; sets $TMPDIR automatically
        tmpdir=lscratch_tmpdir                    #pay attendation to specify it here!
    shell:
        "my_tool --tmp $TMPDIR ..."
```

### GPUs

The executor plugin natively maps `gpu` + `gpu_model` to `--gres=gpu:MODEL:N`:

```python
rule gpu_job:
    output: "results/gpu_output"
    threads: 8
    resources:
        mem_mb=32768,
        runtime=480,
        slurm_partition="gpu",
        gpu=1,
        gpu_model="a100"        # → --gres=gpu:a100:1
    shell:
        "my_gpu_tool ..."
```

Without `gpu_model`, any available GPU is requested (`--gres=gpu:1`).

For constraint-style selection (the old `[gpua100|gpuv100x]` syntax from `bw_submit.py`),
use `slurm_extra`:

```python
resources:
    slurm_partition="gpu",
    gpu=2,
    slurm_extra="'--constraint=[gpua100|gpuv100x]'"
```

### MPI / multinode jobs

The old `ntasks` resource is no longer supported directly. Use `slurm_extra`:

```python
rule mpi_job:
    output: "results/mpi_output"
    threads: 1
    resources:
        mem_mb=8192,
        runtime=600,
        slurm_partition="multinode",
        slurm_extra="'--ntasks=32 --nodes=2'"
    shell:
        "mpirun -np 32 my_mpi_tool ..."
```

---

## Full example Snakefile

```python
rule all:
    input:
        "tests/norm",
        "tests/quick",
        "tests/force_norm",
        "tests/gpu",
        "tests/gpu2",
        "tests/scratch",
        "tests/ntasks",

rule norm:
    output: "tests/norm"
    threads: 10
    resources:
        runtime=600,
        mem_mb=1024
    shell: "echo $TMPDIR; touch {output}"

rule quick:
    output: "tests/quick"
    threads: 4
    resources:
        runtime=10,
        mem_mb=1024,
        disk_mb=10240,
        slurm_extra="'--gres=lscratch:10'",
        tmpdir=lscratch_tmpdir                    #pay attendation to specify it here!
    shell: "touch {output}"

rule force_norm:
    output: "tests/force_norm"
    threads: 10
    resources:
        runtime=10,
        mem_mb=1024,
        slurm_partition="norm"
    shell: "touch {output}"

rule gpu:
    output: "tests/gpu"
    threads: 8
    resources:
        runtime=60,
        mem_mb=16384,
        slurm_partition="gpu",
        gpu=1,
        gpu_model="a100"
    shell: "touch {output}"

rule gpu2:
    output: "tests/gpu2"
    threads: 8
    resources:
        runtime=60,
        mem_mb=16384,
        slurm_partition="gpu",
        gpu=2,
        slurm_extra="'--constraint=[gpua100|gpuv100x]'"
    shell: "touch {output}"

rule scratch:
    output: "tests/scratch"
    threads: 4
    resources:
        runtime=30,
        mem_mb=4096,
        disk_mb=51200,
        slurm_extra="'--gres=lscratch:50'",
        tmpdir=lscratch_tmpdir                    #pay attendation to specify it here!
    shell: "echo $TMPDIR; touch {output}"

rule ntasks:
    output: "tests/ntasks"
    threads: 1
    resources:
        runtime=60,
        mem_mb=4096,
        slurm_partition="multinode",
        slurm_extra="'--ntasks=16 --nodes=2'"
    shell: "touch {output}"
```

---

