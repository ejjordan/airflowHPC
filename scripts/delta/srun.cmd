srun -A bblj-delta-cpu -N 10 --tasks-per-node=128 -p cpu --time=10:00:00 --mem=0 --exclusive --pty bash
salloc -A bblj-delta-cpu --tasks-per-node=128 -p cpu --time=2:00:00 --mem=0 --exclusive -N 2
