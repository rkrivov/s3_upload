#!/usr/bin/env bash

WORK_FOLDER="${HOME}/PycharmProjects/s3_upload"
CURRENT_FOLDER="$(pwd)"

if [ "x${WORK_FOLDER}x" != "x${CURRENT_FOLDER}x" ]; then
  cd "${WORK_FOLDER}" || exit

  trap 'cd "${CURRENT_FOLDER}"' EXIT
  trap 'cd "${CURRENT_FOLDER}"; trap - INT; kill -s INT "$$"' INT
fi

AWS_ACCESS_KEY_ID="nsNJzeUb3GENgWoNh85533"
AWS_SECRET_ACCESS_KEY="71oRDK5qFpdW97tfzZB985PbyTfiWuqhJdBdKCbri2fQ"

venv/bin/python parallels.py "$@"

exit 0
