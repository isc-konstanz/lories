#!/bin/sh
#Scriptname: build.sh
#Description: script to build lories debian packages with dpkg

if [ $(id -u) != 0 ]; then
    echo "DPKG build process should be performed with root privileges." 1>&2
    exit 1
fi

# Attempt to set lories_dir
# Resolve links: $0 may be a link
dir="$0"
# Need this for relative symlinks.
while [ -h "$dir" ] ; do
	ls=`ls -ld "$dir"`
	link=`expr "$ls" : '.*-> \(.*\)$'`
	if expr "$link" : '/.*' > /dev/null; then
		dir="$link"
	else
		dir=`dirname "$dir"`"/$link"
	fi
done
cd "`dirname \"$dir\"`" >/dev/null
lories_dir="`pwd -P`"
build_dir="$lories_dir/build/dpkg/lories"

rm -rf $build_dir
mkdir -p $build_dir

cd $build_dir
cp -r "$lories_dir/lib/debian" "$build_dir/"
chmod 755 "$build_dir/debian/pre*" 2>/dev/null
chmod 755 "$build_dir/debian/post*" 2>/dev/null
chmod 755 "$build_dir/debian/rules"

version=$(cat "$build_dir/debian/version" | tr -d "\n")
rm "$build_dir/debian/version"

sed -i "s/<version>/$version/g" "$build_dir/debian/changelog"
sed -i "s/<version>/$version/g" "$build_dir/debian/control"
sed -i "s/<version>/$version/g" "$build_dir/debian/postinst"

dpkg-buildpackage -us -uc
exit 0
