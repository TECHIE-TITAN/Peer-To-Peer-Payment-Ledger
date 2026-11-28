#!/bin/bash
# Runs the three experiments on one RCE compute node (all 5 LedgerNet nodes and the load generator on it).
#
#   sbatch cluster/run_rce_experiments.sh          # from the repository root, with ~/ln-venv set up
#   squeue -u $USER                                # is it running?
#   tail -f rce-run-<jobid>.log                    # follow it
#   scancel <jobid>                                # stop it (the trap below cleans up)
#
# Node data goes to a local directory (LEDGERNET_TMP, default /tmp), never to the shared /home: fsync on a
# network filesystem would distort every latency number. The script records where it ran so the report can say.
#SBATCH -J ledgernet-exp
#SBATCH -N 1
#SBATCH -c 12
#SBATCH --time=04:00:00
#SBATCH --output=rce-run-%j.log
##SBATCH --exclusive     # uncomment for a cleaner measurement; the job may then wait a long time for a free node

set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$PWD}"
source "$HOME/ln-venv/bin/activate"

export TMPDIR="${LEDGERNET_TMP:-/tmp}/ledgernet-$USER-${SLURM_JOB_ID:-manual}"
export GRPC_ENABLE_FORK_SUPPORT=0
mkdir -p "$TMPDIR" results
THREADS_LOG="$TMPDIR/threads.log"

cleanup() {
    kill "$MONITOR" 2>/dev/null
    pkill -TERM -u "$USER" -f "ledgernet.node" 2>/dev/null
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

describe() {
    echo "== $1"
    echo "host: $(hostname)   cpus allocated: ${SLURM_CPUS_ON_NODE:-?}   nproc: $(nproc)"
    echo "uptime: $(uptime)"
    echo "python: $(python --version 2>&1)   sqlite: $(python -c 'import sqlite3; print(sqlite3.sqlite_version)')"
    echo "TMPDIR=$TMPDIR   filesystem type: $(stat -f -c %T "$TMPDIR")   ulimit -u: $(ulimit -u)"
    df -h "$TMPDIR" | tail -1
}

describe "start" | tee results/rce-environment.txt
(while true; do ps -u "$USER" -L --no-headers | wc -l >> "$THREADS_LOG"; sleep 1; done) &
MONITOR=$!

python -m experiments.failover --runs 10 --platform rce
python -m experiments.latency --rates 50 100 200 400 800 1200 1600 2000 --platform rce
python -m experiments.recovery --lengths 10000 50000 100000 --intervals 0 1000 --runs 5 --platform rce

{
    describe "end"
    echo "peak threads of this user during the run: $(sort -n "$THREADS_LOG" | tail -1)"
} | tee -a results/rce-environment.txt
