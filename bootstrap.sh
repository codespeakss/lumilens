#!/bin/bash

# 定义目录路径
DIR="./"  # 当前目录

# 查找并替换
grep -rl "192.168.18.101" "$DIR" | while read -r file; do
    sed -i '' 's/192.168.18.101/192.168.18.101/g' "$file"
    echo "Updated $file"
done

echo "Replacement complete!"

