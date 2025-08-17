#!/bin/bash

# Default .env file path
ENV_FILE=".env"

# Use argument if provided
if [ -n "$1" ]; then
  ENV_FILE="$1"
fi

# Check if file exists
if [ ! -f "$ENV_FILE" ]; then
  echo "❌ File $ENV_FILE not found!"
  exit 1
fi

# Read the .env file line by line
# Skip empty lines and comments
while IFS= read -r line; do
  # Trim leading/trailing whitespace
  line=$(echo "$line" | xargs)

  # Skip if line is empty or starts with #
  if [[ -z "$line" || "$line" == \#* ]]; then
    continue
  fi

  # Export the variable
  export "$line"
done < "$ENV_FILE"

echo "✅ Environment variables loaded from $ENV_FILE"
