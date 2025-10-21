#!/bin/sh
#Scriptname: build.sh
#Description: script to build lories debian packages with dpkg

if [ "$(id -u)" != 0 ]; then
    echo "DPKG build process should be performed with root privileges." 1>&2
    exit 1
fi

# Attempt to set lories_dir
# Resolve links: $0 may be a link
dir="$0"
# Need this for relative symlinks.
while [ -h "$dir" ] ; do
	ls=$(ls -ld "$dir")
	link=$(expr "$ls" : '.*-> \(.*\)$')
	if expr "$link" : '/.*' > /dev/null; then
		dir="$link"
	else
		dir=$(dirname "$dir")"/$link"
	fi
done
cd "$(dirname "$dir")" || exit 1 >/dev/null
lories_dir="$(pwd -P)"
build_dir="$lories_dir/build/dpkg"

# Attempt to determine the Python command
if [ -x "$lories_dir/.venv/bin/python" ] ; then
	python="$lories_dir/.venv/bin/python"
else
	python="/usr/bin/python"
fi
if [ ! -x "$python" ] ; then
    die "ERROR: Python is set to an invalid entry point: $python

Please setup a local virtual environment '.venv' or Python 3 to be available as '/usr/bin/python'."
fi

rm -rf "$build_dir"
mkdir -p "$build_dir/lories"

cd "$build_dir/lories" || exit 1 >/dev/null
cp -r "$lories_dir/lib/debian" "$build_dir/lories/"
chmod 755 "$build_dir/lories/debian/pre*" 2>/dev/null
chmod 755 "$build_dir/lories/debian/post*" 2>/dev/null
chmod 755 "$build_dir/lories/debian/rules"

version=$($python "$lories_dir/setup.py" --version)
if [ -z "$version" ]; then
    echo "Could not determine version from setup.py" 1>&2
    exit 1
elif echo "$version" | grep -q '\.dirty$'; then
	echo "Invalid determined dirty version from setup.py: $version" 1>&2
	exit 1
elif echo "$version" | grep -q '\+'; then
    version=$(echo $version | sed "s/[+].*//")
fi
sed -i "s/<version>/$version/g" "$build_dir/lories/debian/changelog"
sed -i "s/<version>/$version/g" "$build_dir/lories/debian/control"
sed -i "s/<version>/$version/g" "$build_dir/lories/debian/postinst"

dpkg-buildpackage -us -uc
exit 0
