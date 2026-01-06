
module load cray-python/3.10.10
module list

export SCALEMS="/pfs/lustrep3/scratch/project_465001666/pekasson"

cd $SCALEMS
. airflow_env/bin/activate

# no idea why this is sometimes needed
export PYTHONPATH=$SCALEMS/airflowHPC

export PATH="$PATH:$HOME/j/spack/bin"
. /pfs/lustrep1/appl/lumi/spack/23.09/0.21.0-user/share/spack/setup-env.sh
module load spack
eval `spack load --sh   /saecpmy`
spack load postgresql
spack load gromacs

