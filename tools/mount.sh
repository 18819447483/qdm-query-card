#!/usr/bin/env bash
# 把源仓库的 plugin/ 挂到 qwenpaw 插件运行时目录。
#
# 为什么要挂而不是复制：宿主 loader 从外部路径安装时会 rmtree + copytree
# （plugins/loader.py:1159-1169），等于每改一行代码都要重装一次。挂链接后
# 改动即时生效，只需在宿主侧 reload 插件。
#
# 用法：  ./tools/mount.sh
# 卸载：  删除 ~/.qwenpaw/plugins/qdm-query-card 即可（源仓库不受影响）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/plugin"
DST="$HOME/.qwenpaw/plugins/qdm-query-card"

if [ ! -d "$SRC" ] || [ ! -f "$SRC/plugin.json" ]; then
	echo "error: $SRC does not look like a plugin dir (missing plugin.json)" >&2
	exit 1
fi

if [ -e "$DST" ] || [ -L "$DST" ]; then
	echo "already mounted: $DST"
	exit 0
fi

case "$(uname -s)" in
CYGWIN* | MINGW* | MSYS*)
	SRC_WIN="$(cygpath -w "$SRC")"
	DST_WIN="$(cygpath -w "$DST")"
	powershell -NoProfile -Command \
		"New-Item -ItemType Junction -Path '$DST_WIN' -Target '$SRC_WIN' | Out-Null"
	;;
*)
	ln -s "$SRC" "$DST"
	;;
esac

echo "mounted: $DST -> $SRC"
echo "next: restart/reload qwenpaw so it picks up plugin id 'qdm-query-card'"
