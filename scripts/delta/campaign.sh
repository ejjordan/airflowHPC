#!/bin/bash

# load modules, spack, python env
. ./prepare.sh > prepare.log 2>&1


# ------------------------------------------------------------------------------
#
# basic settings
#
MODE='no-rct'

DAG='gmx_multi'
DAG='swarms'
DAG='anthracene_runner'



DAGF=$(grep -l "\"$DAG\"" /u/merzky/scalems/airflowHPC/airflowHPC/dags/*py)

echo "DAGF: $DAGF"

test -z "$DAGF" && DAGF="$DAG"

if test "$MODE" == 'rct'; then
    DAG="rct_$DAG"
fi

echo "=== DAG: $DAG"


export SCALEMS="$HOME/scalems"
export AIRFLOW="$HOME/airflow"

export OMP_PLACES=cores
export TMPDIR=$SCALEMS/tmp
export RUNS=$SCALEMS/runs

cd $SCALEMS
mkdir -p $RUNS 
mkdir -p $TMPDIR


# ------------------------------------------------------------------------------
#
db_start(){
    echo '========================== db start'

    echo "clean log files etc"
    rm -f /tmp/.s.PGSQL*
    rm -rf    $SCALEMS/postgresql_db/data/*
    rm -f     $SCALEMS/postgresql_db/*.log

    initdb    $SCALEMS/postgresql_db/data
    pg_ctl -D $SCALEMS/postgresql_db/data/ -l $SCALEMS/postgresql_db/server.log start
    createdb -T template1 airflow_db
    psql airflow_db <<EOT
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT ALL ON SCHEMA public TO airflow_user;
ALTER USER airflow_user SET search_path = public;
EOT

    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db"

    echo '========================== db start ok'
}


# ------------------------------------------------------------------------------
#
db_stop(){
    echo '========================== db stop'
    
    rm -f /tmp/.s.PGSQL*

    killall -9 postgres
    pg_ctl -D $SCALEMS/postgresql_db/data/ stop

    echo '========================== db stop ok'
}


# ------------------------------------------------------------------------------
#
airflow_start(){
    echo '========================== airflow start'

    # make sure we start from an empty slate
    airflow_stop

    nodes=$1
    slots=$2
    cpn=$3
    gpn=$4

    echo "=== start airflow ($slots slots)"

    echo 'clean log files etc.'
    rm -rf rp.session.*
    # rm -rf ~/j/sbox/rp.session.*
    rm -rf $AIRFLOW/*.{out,err,log,pid}
    rm -rf $AIRFLOW/logs/*
    rm -rf $SCALEMS/tmp/{tmp,rp.ompi}*
    rm -rf $RUNS/*


    db_start


    if test "$MODE" == 'rct'; then
        echo "=== using RadicalExecutor"
        export AIRFLOW__CORE__EXECUTOR=airflowHPC.executors.radical_executor.RadicalExecutor
    else
        echo "=== using ResourceExecutor"
        export AIRFLOW__CORE__EXECUTOR=airflowHPC.executors.resource_executor.ResourceExecutor
    fi

    
    # TODO: threads_per_core is not passed
    export AIRFLOW__HPC__CORES_PER_NODE=$cpn
    export AIRFLOW__HPC__GPUS_PER_NODE=$gpn
    export AIRFLOW__HPC__GPU_TYPE="nvidia"
    export AIRFLOW__HPC__MEM_PER_NODE=256
    export AIRFLOW__HPC__THREADS_PER_CORE=1

    export AIRFLOW__CORE__PARALLELISM=$slots
    export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=$slots
    export AIRFLOW__CORE__PDAG_CONCURRENCY=$slots
    export AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1

    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__DAGS_FOLDER="$SCALEMS/airflowHPC/airflowHPC/dags/"

    # TODO: check this setting
    export AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY=$slots
    export AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR=True

    export RCT_PILOT_CFG=$SCALEMS/pilot_cfg.json
    export RCT_PARALLELISM=$slots
    export RADICAL_UTILS_NO_ATFORK=1

    export SLURM_TASKS_PER_NODE=128
    export SLURM_CPUS_PER_TASK=1

    # dag level settings:
    #     max_active_tasks=$slots
    #     concurrency=$slots
    #     max_active_runs=1

    echo "start scheduler"
    airflow db init
    airflow db migrate 

    echo "==== DB PREP DONE"
    date

    airflow scheduler -D
    echo "==== SCHED STARTED"
    date
    
    airflow pools set default_pool $slots test
    airflow pools list

    echo 'reparse dags'
    AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=0 \
        airflow dag-processor -n 1 -S $DAGF
    
    echo "unpause $DAG"
    airflow dags unpause $DAG
    echo "list dags"
    airflow dags list
    echo "list errors"
    airflow dags list-import-errors
 
    echo 'trigger anthracene_runner'
    airflow dags trigger -v $DAG
  # airflow dags backfill --reset-dagruns -y -s '2025-01-01' \
  #                       --conf="{\"output_dir\" : \"$RUNS\"}" "$DAG"
    echo '========================== airflow start ok'
    date

}


# ------------------------------------------------------------------------------
#
airflow_stop() {
    echo '========================== airflow stop'
    spid=$(cat $AIRFLOW/airflow-scheduler.pid) 
    echo "kill scheduler $spid"
    kill $spid
    sleep 1

    echo 'clean rp tasks'
    for pid in $(ps -ef | grep -e rp. | grep -v grep | grep merzky | cut -c 8-16)
    do ps h -ef -q $pid;
        kill -9 $pid
    done

    echo 'clean airflow tasks'
    for pid in $(ps -ef | grep airflow | grep -v grep | cut -c 8-16)
    do
        kill -9 $pid
    done
    ps -ef | grep gunicorn | grep -v grep | cut -c 8-16 | xargs kill
    
    db_stop

    echo '========================== airflow stop ok'

    
  # echo 'clean log files etc.'
    # rm -rf rp.session.*
    # rm -rf ~/j/sbox/rp.session.*
    # rm -rf ~/airflow/*.{out,err,log,pid}
    # rm -rf ~/airflow/logs/*
  # rm -rf $SCALEMS/runs/*
  # rm -rf $SCALEMS/tmp/{tmp,rp.ompi}*
}


# ------------------------------------------------------------------------------
#
run_exp(){

    echo '========================== exp_run [$@]'

    export SCALEMS_EXPERIMENT=$1
    export SCALEMS_N_NODES=$2
    export SCALEMS_N_SLOTS=$3
    export SCALEMS_N_TASKS=$4

    sbox="sbox_${SCALEMS_EXPERIMENT}_${DAG}_${SCALEMS_N_NODES}_${SCALEMS_N_SLOTS}_${SCALEMS_N_TASKS}"

    echo '---------------------------------------------------------------------'
    echo "run $SCALEMS_EXPERIMENT N:$SCALEMS_N_NODES S:$SCALEMS_N_SLOTS T:$SCALEMS_N_TASKS: "
    echo "use P:$RCT_PARALLELISM"
    echo "SBOX: $sbox"

    if test -d $sbox
    then
        echo "sandbox exists - skip [$sbox]"
        return
    fi

    airflow_start $SCALEMS_N_NODES $SCALEMS_N_SLOTS 128 0

    while true
    do
        line=$(grep 'DagRun Finished' $AIRFLOW/airflow-scheduler.log)
        if test -z "$line"
        then
            echo -n .
            sleep 10
            continue
        fi

        echo "==== completion: $line"

        echo
        duration=$(echo "$line" | sed -e 's/.*duration=//g' | cut -f 1 -d ,)
        echo "ok  $SCALEMS_N_SLOTS  $SCALEMS_N_TASKS  $duration"
        echo "$SCALEMS_N_SLOTS  $SCALEMS_N_TASKS  $duration" >> results_$SCALEMS_EXPERIMENT.dat


        mkdir -p "$sbox"
        cp *log "$sbox"
        cp -r "$AIRFLOW/" "$sbox"
        cp -r runs "$sbox"

        sid=$(ls -rtd rp.session* | tail -n 1)
        if test -z "$sid"
        then
            echo "no RP session"
        else
            echo "SID : $sid"
            mkdir -p $sbox
            mv $sid $sbox/
            mv $HOME/j/sbox/$sid $sbox/$sid.pilot
            mv campaign.log $sbox/
        fi

        break
    done

    airflow_stop
    echo '========================== exp_run ok [$@]'
}

run_exp test_1 1  4 16
# run_exp test_2 1 16 16
# 
# # weak scaling
# for n in 32 64 128 256 512; do
#     run_exp weak 10 $n $n
# done
#
# for n in 32 64 128 256 512; do
#     run_exp strong_1 10 $n 512
# done
#
# for n in 32 64 128 256 512; do
#     run_exp strong_2 10 $n $((512 * 4))
# done


echo "cleanup"

