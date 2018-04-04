#!/bin/bash
topdir="$DistJETPATH/Backend/HTCONDOR"
confdir="$HOME/.DistJET"
if [ ! -d $confdir ]; then
	mkdir $confdir
fi
dir="$confdir/ssh-auth"

if [ ! -d $dir ]; then
    mkdir $dir
fi

cd $dir
if [ ! -f "ip.txt" ]; then
	touch ip.txt
fi
auth="$dir/authority"
if [ ! -d $auth ]; then
	mkdir "$auth"
fi
cd "$auth"

#export SSHTOP="$HOME/.DistJET/ssh_auth/authorized"
export SSHTOP=$(pwd)
export USERLIST=$USER
export PORT=${PORT:-2222}

function create-sshd-config() {
    local file=$1

    cat <<EOF > $file
UsePrivilegeSeparation no 
Protocol 2
HostKey ${SSHTOP}/ssh_host_rsa_key
#HostbasedAuthentication yes
RequiredAuthentications2 publickey
RSAAuthentication yes
PubkeyAuthentication yes
AuthorizedKeysFile ${SSHTOP}/authorized_keys
AllowUsers ${USERLIST}
EOF
}

function check-authorized-file() {
	local file="${SSHTOP}/authorized_keys"
	if [ ! -f "$file" ]; then
#		touch $file
		cat "ssh_host_rsa_key.pub">>$file
	fi
}

function check-sshd-config() {
    local file=$1
    if [ -f "$file" ]; then
        echo $(readlink -f $file)
        return
    fi
    
    create-sshd-config $file
}

function check-host-keys() {

    for t in rsa ecdsa ;
    do
        local f=ssh_host_${t}_key
        [ -f "$f" ] || ssh-keygen -t ${t} -f ${f}
    done
}
check-sshd-config
check-host-keys
check-authorized-file
