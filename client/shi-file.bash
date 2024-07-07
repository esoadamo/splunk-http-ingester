#!/bin/bash
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <file_path> <source_type> <source>"
    exit 1
fi

set -Eeuo pipefail

print_error_line() {
    echo "An error occurred on line $1"
}

trap 'print_error_line $LINENO' ERR

cd "$(dirname "$(realpath "$0")")"

file_path="$1"
source_type="$2"
source="$3"
encoded_file_path=$(echo -n "$file_path" | md5sum | sed 's/^\([a-f0-9]*\).*/\1/')
metadata_dir="$HOME/.cache/shi-file"
metadata_file="${metadata_dir}/${encoded_file_path}.metadata"

mkdir -p "$metadata_dir"

if [ ! -f "$file_path" ]; then
    echo "File does not exist: $file_path"
    exit 1
fi

if [ ! -f "$metadata_file" ]; then
    echo "0" > "$metadata_file"
    echo "00000000000000000000000000000000" >> "$metadata_file"
    uuidgen >> "$metadata_file"
fi

last_line=$(head -n 1 "$metadata_file")
last_md5=$(sed -n '2p' "$metadata_file")
file_uuid=$(head -n 3 "$metadata_file" | tail -n 1)

# Remove trailing empty lines before calculating MD5 and line count
file_content=$(sed '/^$/d' "$file_path")

if [ "$last_line" -gt 0 ]; then
    current_line_md5=$(echo "$file_content" | sed -n "${last_line}p" | md5sum | sed 's/^\([a-f0-9]*\).*/\1/')
else
    current_line_md5="00000000000000000000000000000000"
fi
total_lines=$(echo "$file_content" | wc -l)
current_last_line=$(echo "$file_content" | tail -n 1 | md5sum | sed 's/^\([a-f0-9]*\).*/\1/')

if [ "$total_lines" -lt "$last_line" ] || [ "$current_line_md5" != "$last_md5" ]; then
    echo "$file_content" | ./shi.bash "$source_type" "$source" "$file_uuid"
else
    lines_to_skip=$((last_line + 1))
    new_lines=$((total_lines - last_line))
    if [ "$new_lines" -gt 0 ]; then
        echo "$file_content" | tail -n +"$lines_to_skip" | ./shi.bash "$source_type" "$source" "$file_uuid"
    fi
fi

echo "$total_lines" > "$metadata_file"
echo "$current_last_line" >> "$metadata_file"