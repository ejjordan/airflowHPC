no_tmp=false
portnum=""
max_connections=512
shared_buffers="1024MB"
spack_path=$(pwd)/spack
while getopts "s:p:m:b:-:th" opt; do
  case ${opt} in
    s)
      echo "Spack path set to $OPTARG"
      spack_path=${OPTARG} ;;
    p)
      echo "Port number set to $OPTARG"
      portnum=${OPTARG} ;;
    m)
      echo "Max connections set to $OPTARG"
      max_connections=${OPTARG} ;;
    b)
      echo "Shared buffers set to $OPTARG"
      shared_buffers=${OPTARG} ;;
    t)
      echo "Will use $HOME for the postgresql database"
      no_tmp=true ;;
    h)
      echo "Usage: cmd [-s spack-path] [-p portnum] [-m max-connections] [-b shared-buffers] [-t no-tmp]"
      echo "  -s [--spack-path]: Location to check for an existing Spack installation or to install Spack to."
      echo "  -p [--portnum]: Port number for postgresql. This can be useful on shared systems if the default port is already in use."
      echo "  -m [--max-connections]: Set the maximum number of connections in postgresql.conf. Default is 512."
      echo "  -b [--shared-buffers]: Set the shared_buffers in postgresql.conf. Default is 1024MB."
      echo "  -t [--no-tmp]: Use $HOME for the postgresql database instead of /tmp."
      ;;
    -) case $OPTARG in
        spack-path)
          spack_path="${!OPTIND}"; OPTIND=$((OPTIND + 1))
          echo "Spack path set to $spack_path" ;;
        portnum)
          portnum="${!OPTIND}"; OPTIND=$((OPTIND + 1))
          echo "Port number set to $portnum" ;;
        max-connections)
          max_connections="${!OPTIND}"; OPTIND=$((OPTIND + 1))
          echo "Max connections set to $max_connections" ;;
        shared-buffers)
          shared_buffers="${!OPTIND}"; OPTIND=$((OPTIND + 1))
          echo "Shared buffers set to $shared_buffers" ;;
        no-tmp)
          echo "Will use $HOME for the postgresql database"
          no_tmp=true ;;
        *)
          echo "Invalid option: '$OPTARG'"
          exit 1
          ;;
      esac
      ;;
    \? )
      echo "Invalid option: '$OPTARG'"
      exit 1
      ;;
  esac
done

# Activate Spack environment and load postgresql
if [ -f "$spack_path/share/spack/setup-env.sh" ]; then
  source "$spack_path/share/spack/setup-env.sh"
  echo "Spack environment setup sourced from $spack_path/share/spack/setup-env.sh"
  spack load postgresql
else
  echo "Error: Unable to source $spack_path/share/spack/setup-env.sh"
  exit 1
fi

# Setup db in postgresql_db directory if it does not exist
if [ ! -d "postgresql_db" ]; then

  mkdir postgresql_db

  db_opts="--set listen_addresses='*' --set max_connections=$max_connections --set shared_buffers=$shared_buffers"
  if [ $no_tmp = true ]; then
    db_opts="$db_opts --set unix_socket_directories='$HOME'"
  fi
  if [ -n "$portnum" ]; then
    db_opts="$db_opts --set port=$portnum"
  fi
  echo "initdb ${db_opts} -D postgresql_db/data"
  initdb_command="initdb ${db_opts} -D postgresql_db/data"
  eval $initdb_command

  pg_ctl -D postgresql_db/data -l postgresql_db/logfile start
  # create user and database
  if [ $portnum == false ]; then
    createdb -T template1 airflow_db
    psql airflow_db -f airflow_db.sql
  else
    createdb -p $portnum -T template1 airflow_db
    psql -p $portnum airflow_db -f airflow_db.sql
  fi
fi
