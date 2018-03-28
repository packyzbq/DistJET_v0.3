#!/bin/bash

#export SSHTOP="$HOME/.DistJET/ssh_auth/authorized"
export SSHTOP=$(pwd)
export USERLIST=$USER
export PORT=${PORT:-2222}
export DEBUG=${DEBUG:-1}

if [ "$DEBUG" == "1" ]; then
    DEBUG=true
else
    DEBUG=false
fi

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

function start-sshd() {
    local file=sshd_config
    check-sshd-config $file
    check-host-keys
	check-authorized-file
    local options="-p $PORT -f $file"

    $DEBUG && options="-d $options"
    #$DEBUG && options="-D $options"
	echo "option = $options"
    /usr/sbin/sshd $options
}

start-sshd
