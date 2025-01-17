#!/bin/bash

# options
spack_path=$(pwd)/spack
install_spack=false
dry_run=false
while getopts "p-:idh" opt; do
  case ${opt} in
    p) spack_path=${OPTARG} ;;
    i) install_spack=true ;;
    d) dry_run=true ;;
    h)
      echo "Usage: cmd [-p spack-path] [-i install]"
      echo "  -p [--spack-path]: Location to check for an existing Spack installation or to install Spack to."
      echo "  -i [--install]: Install Spack to spack-path."
      echo "  -d [--dry-run]: Do not install spack packages or clone Spack repository."
      ;;
    -) case $OPTARG in
        spack-path) spack_path="${!OPTIND}"; OPTIND=$((OPTIND + 1)) ;;
        install) install_spack=true ;;
        dry-run) dry_run=true ;;
        *)
          echo "Invalid option: $OPTARG"
          exit 1
          ;;
      esac
      ;;
    \? )
      echo "Invalid option: $OPTARG"
      exit 1
      ;;
  esac
done

postgresql="postgresql^python@3.12"
if [ $install_spack == true ]; then
  if [ -f "$spack_path/share/spack/setup-env.sh" ]; then
    echo "Spack is already installed in $spack_path"
    source "$spack_path/share/spack/setup-env.sh"
    spack spec $postgresql
    if $dry_run; then
      echo "Dry run: Spack packages will not be installed."
      exit 1
    fi
    spack install $postgresql
    echo "Done configuring Spack"
    exit 1
  fi
  read -p "Spack not found in '$spack_path'. Do you want to clone it to '$spack_path'? (y/n): " confirm
  if [[ "$confirm" =~ ^(yes|y)$ ]]; then
    if $dry_run; then
      echo "Dry run: Spack repository will not be cloned."
      exit 1
    fi
    git clone -c feature.manyFiles=true --depth=2 https://github.com/spack/spack.git "$spack_path"
  else
    echo "Clone operation aborted."
    exit 1
  fi
fi
# Check if Spack is already installed
if [ -f "$spack_path/share/spack/setup-env.sh" ]; then
  source "$spack_path/share/spack/setup-env.sh"
  echo "Spack environment setup sourced from $spack_path/share/spack/setup-env.sh"
  spack spec $postgresql
  if $dry_run; then
    echo "Dry run: Spack packages will not be installed."
    exit 1
  fi
  spack install $postgresql
  echo "Done configuring Spack"
  exit 1
else
  echo "Error: Unable to source $spack_path/share/spack/setup-env.sh"
  exit 1
fi
