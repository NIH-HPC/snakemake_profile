#! /usr/local/bin/python3

"""
Snakemake SLURM submit script convenience wrapper for biowulf

Everything is obtained from task resources & threads without any configuration
files. The biowulf partition is inferred from resource requirements if not
manually specified. Resource names are the conventional names used by other
executors, but aliases to slurm-specific resources are also included
(https://snakemake.readthedocs.io/en/stable/executing/cluster.html#advanced-resource-specifications)

Required resources: mem_mb
Optional resources: disk_mb, gpu, gpu_model, runtime, ntasks, nodes

"""

import argparse
import sys
import os
import shlex
from math import ceil
from subprocess import run
from snakemake.utils import read_job_properties


def assign_partition(threads, mem_mb, time_min, gres, ntasks, nodes):
    """
    Assign a partition to the job. This should roughly work in many cases
    but might need to be adapted. It ignores buyin partitions, for
    example.
    """
    if any(a.startswith("gpu:") for a in gres):
        return "gpu"
    if ntasks is not None and ntasks > 16:
        return "multinode"
    if nodes is not None and nodes > 1:
        return "multinode"
    if time_min <= 120 and mem_mb <= 370 * 1024:
        return "quick"
    if time_min <= 240 * 60 and mem_mb <= 499 * 1024:
        return "norm"
    if time_min > 240 * 60:
        return "unlimited"
    return "largemem"


def make_sbatch_cmd(props):
    try:
        rule = props["rule"]
    except KeyError:
        rule = props["groupid"]
    resources = props["resources"]
    # profile_dir = Path(__file__).resolve().parent

    # defaults
    threads = props.get("threads", 2)
    mem_mb = None
    time_min = 120
    gres = []
    ntasks = None
    nodes = None

    sbatch_cmd = ["sbatch", f"--cpus-per-task={threads}"]

    def as_int(key):
        """
        Convert resource to integer; if it can't be converted then exit 1.
        """
        try:
            val = int(resources[key])
        except ValueError:
            print(f"{rule}: Could not parse {key}={resources[key]}", file=sys.stderr)
            sys.exit(1)
        return val

    # Snakemake recommended resource name is 'tasks' rather than 'ntasks', but
    # retain backwards compatibility
    # (https://snakemake.readthedocs.io/en/stable/executing/cluster.html#advanced-resource-specifications)
    if "tasks" in resources:
        resources["ntasks"] = resources["tasks"]

    if "ntasks" in resources:
        ntasks = as_int("ntasks")
        sbatch_cmd.append(f"--ntasks={ntasks}")

    if "nodes" in resources:
        nodes = as_int("nodes")
        sbatch_cmd.append(f"--nodes={nodes}")

    if "mem_mb" in resources:
        mem_mb = as_int("mem_mb")
        sbatch_cmd.append(f"--mem={mem_mb}")
    else:
        print(f"{rule}: ERROR - mem_mb is required to be in resources", file=sys.stderr)
        sys.exit(1)

    if "runtime" in resources:
        time_min = as_int("runtime")

    # Use default if not otherwise specified
    sbatch_cmd.append(f"--time={time_min}")

    if "disk_mb" in resources:
        disk_mb = as_int("disk_mb")
        disk_gb = ceil(disk_mb / 1024.0)
        gres.append(f"lscratch:{disk_gb}")

    if "gpu" in resources:
        if "gpu_model" in resources:
            model = resources["gpu_model"]
            # allow the definition of a constraint instead of a single gpu model.
            if "|" in model:
                gres.append(f'gpu:{resources["gpu"]}')
                sbatch_cmd.append(f"--constraint={model}")
            else:
                gres.append(f'gpu:{resources["gpu_model"]}:{resources["gpu"]}')
        else:
            gres.append(f'gpu:{resources["gpu"]}')

    if len(gres) > 0:
        sbatch_cmd.append(f'--gres={",".join(gres)}')

    if "slurm_partition" in resources:
        partition = resources["slurm_partition"]
    else:
        partition = assign_partition(threads, mem_mb, time_min, gres, ntasks, nodes)

    sbatch_cmd += [
        f"--output=logs/{rule}-%j.out",
        f"--partition={partition}",
    ]

    if "slurm_extra" in resources:
        sbatch_cmd.extend(shlex.split(f'{resources["slurm_extra"]}'))

    return sbatch_cmd, rule


if __name__ == "__main__":
    # minimal commandline - single argument is the snakemake submit script
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("jobscript", help="Snakemake jobscript with job properties.")
    jobscript = p.parse_args().jobscript
    props = read_job_properties(jobscript)

    # make sure log dir exists
    try:
        os.mkdir("logs")
    except FileExistsError:
        pass
    except OSError as err:
        print(err, file=sys.stderr)
        sys.exit(1)

    sbatch_cmd, rule = make_sbatch_cmd(props)
    sbatch_cmd.append(jobscript)
    print(f'{rule}: submission command "{" ".join(sbatch_cmd)}', file=sys.stderr)

    sbatch_res = run(sbatch_cmd, capture_output=True, encoding="utf-8")
    if sbatch_res.returncode == 0:
        print(sbatch_res.stdout)
        sys.exit(0)
    else:
        print(
            f"----- Submission failed for a rule {rule} execution -----",
            file=sys.stderr,
        )
        print(sbatch_res.stdout, file=sys.stdout)
        print(sbatch_res.stderr, file=sys.stderr)
        print(
            "----------------------------------------------------------",
            file=sys.stderr,
        )
        sys.exit(sbatch_res.returncode)
