#!/bin/sh
#Scriptname: build.sh
#Description: script to build lori debian packages with dpkg

if [ $(id -u) != 0 ]; then
    echo "DPKG build process should be performed with root privileges." 1>&2
    exit 1
fi

# Attempt to set lori_dir
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
lori_dir="`pwd -P`"
build_dir="$lori_dir/build/dpkg/lori"

rm -rf $build_dir
mkdir -p $build_dir

cd $build_dir
cp -r "$lori_dir/lib/debian" "$build_dir/"
chmod 755 "$build_dir/debian/pre*" 2>/dev/null
chmod 755 "$build_dir/debian/post*" 2>/dev/null
chmod 755 "$build_dir/debian/rules"

version=$(cat "$build_dir/debian/version" | tr -d "\n")
rm "$build_dir/debian/version"

sed -i "s/<version>/$version/g" "$build_dir/debian/changelog"
sed -i "s/<version>/$version/g" "$build_dir/debian/control"
sed -i "s/<version>/$version/g" "$build_dir/debian/postinst"

cp "$lori_dir/lib/systemd/lori.service" "$build_dir/debian/"

cp -r "$lori_dir/lib/tmpfiles.d" "$build_dir/"
cp -r "$lori_dir/lib/logrotate.d" "$build_dir/"

mkdir -p "$build_dir/etc"
cp -r "$lori_dir/conf/logging.default.conf" "$build_dir/etc/logging.conf"
cp -r "$lori_dir/conf/settings.default.conf" "$build_dir/etc/settings.conf"

dpkg-buildpackage -us -uc
exit 0
