#!/usr/bin/env bash

echo "------------------------------------------------"
echo "Application was started from" $(whoami)
echo "User home folder is" ${HOME}
echo "AWS security folder is ${HOME}/.aws/"
echo "------------------------------------------------"

echo ""

WORK_FOLDER="${HOME}/PycharmProjects/s3_upload"
CURRENT_FOLDER="$(pwd)"

if [ "x${WORK_FOLDER}x" != "x${CURRENT_FOLDER}x" ]; then
  cd "${WORK_FOLDER}" || exit

  trap 'cd "${CURRENT_FOLDER}"' EXIT
  trap 'cd "${CURRENT_FOLDER}"; trap - INT; kill -s INT "$$"' INT
fi

venv/bin/python main.py "$@"

exit 0
