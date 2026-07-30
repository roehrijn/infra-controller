#!/bin/bash

SCRIPTNAME=`basename "$0"`
HBN_CONFIG_DIR="/var/lib/hbn"

# create default HBN directories, in order to mount those in container
mkdir -p /var/lib/hbn/var/support/
mkdir -p /var/log/doca/hbn
mkdir -p /var/lib/hbn/etc/nvue.d
mkdir -p /var/lib/hbn/etc/cumulus
mkdir -p /var/lib/hbn/var/lib/nvue
mkdir -p /var/lib/hbn/etc/supervisor/conf.d
mkdir -p /var/lib/hbn/etc/frr

usage () {
    echo "usage: $SCRIPTNAME"
    echo "$SCRIPTNAME -m|--mtu <MTU> Use <MTU> bytes for all HBN interfaces (default 9216)"
    echo "$SCRIPTNAME -u|--username <username> User creation"
    echo "$SCRIPTNAME -p|--password <password> Password for --username <username>"
    echo "$SCRIPTNAME -e|--enable-rest-api-access Enable REST API from external IPs"
	echo "$SCRIPTNAME -h|--help"
    exit 0
}

createHbnUsers() {
    mkdir -p "$HBN_CONFIG_DIR"/etc/hbn-users
}

hbnUsersDir="${HBN_CONFIG_DIR}/etc/hbn-users"
hbnNvueDir="${HBN_CONFIG_DIR}/etc/nvue.d"
hbnCumulusDir="${HBN_CONFIG_DIR}/etc/cumulus"
hbnDpuSetupConf="${hbnCumulusDir}/hbn-dpu-setup.conf"
hbnNvueStartup="${hbnNvueDir}/startup.yaml"
defUsername='nvidia'
defUsernameFile="${hbnUsersDir}/${defUsername}.pass"

uflag=false
pflag=false

createHbnUsers

appendStartupYaml() {
    sed -Ei 's/(:.*)([0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2})/\1'\''\2'\''/g' $hbnNvueStartup
    /usr/bin/python3 enable-rest-api.py
    sed -i 's/: true/: on/g' $hbnNvueStartup
    sed -i 's/: false/: off/g' $hbnNvueStartup
}

enableRest () {
    echo "Enabling REST API access from external IPs"
    if [ ! -f $hbnNvueStartup ]; then
        cp etc/nvue.d/enable-rest.yaml $hbnNvueStartup
    elif [ ! -s $hbnNvueStartup ]; then
        cp etc/nvue.d/enable-rest.yaml $hbnNvueStartup
    else
        echo "Extisting startup configuration present, will append the API configuration"
        appendStartupYaml
    fi
    #To change the default immutables.yaml copying following configuration
    cp etc/cumulus/hbn-dpu-setup.conf $hbnDpuSetupConf
}

VALID_ARGS=$(getopt -o m:u:p:eh --long  mtu:,username:,password:,enable-rest-api-access,help -- "$@")

if [[ $? -ne 0 ]]; then
    exit 1;
fi

eval set -- "$VALID_ARGS"

while [ : ]; do
    case "$1" in
        -m | --mtu ) mtu=$2; shift 2;;
        -u | --username ) username=$2; uflag=true; shift 2;;
        -p | --password ) password=$2; pflag=true; shift 2;;
        -e | --enable-rest-api-access ) enable_rest=true; shift;;
        -h | --help ) usage; shift; exit;;
        -- ) shift; break;;
    esac
done

addUser () {
	username=$1
	password=$2
	/usr/bin/python3 encrypt_password.py -u $username -p $password
}

if [[ $enable_rest == true ]]; then
    if [ ! -f $defUsernameFile ]; then
        if [[ ( $uflag == true && $pflag == true ) ]]; then
            if [[ $username == 'nvidia' ]]; then
                if [[ $password == 'nvidia' ]]; then
                    echo "Warning: Entered password is same as default password"
                    echo "Please use different password"
                    exit 0
                fi
                enableRest
                addUser $username $password
            else
                echo "Please enter default username nvidia to change the default password"
            fi
        else
            echo "To enable REST API access please change the default password of nvidia user account"
            usage
        fi
    else
        enableRest
        if [[ ( $uflag == true && $pflag == true ) ]]; then
            if [[ $username == 'nvidia' ]]; then
                if [[ $password == 'nvidia' ]]; then
                    echo "Warning: Entered password is same as default password"
                    echo "Please use different password"
                    exit 0
                fi
                addUser $username $password
            else
                 echo "Please enter default username nvidia to change the default password"
            fi
        fi
    fi
else
    if [[ ( $uflag == true && $pflag == true ) ]]; then
        if [[ $username == 'nvidia' ]]; then
            if [[ $password == 'nvidia' ]]; then
                echo "Warning: For user nvidia entered password is same as default password"
                echo "Please use different password"
                exit 0
            fi
        fi
        addUser $username $password
    else
        if [[ ( $uflag == true && $pflag == false ) ]]; then
            usage
        elif [[ ( $uflag == false && $pflag == true ) ]]; then
            usage
        fi
    fi
fi


mkdir -p "$HBN_CONFIG_DIR"/etc/network/
touch "$HBN_CONFIG_DIR"/etc/network/interfaces

if ! grep -q "iface p0" "$HBN_CONFIG_DIR"/etc/network/interfaces; then
	cp network.interfaces "$HBN_CONFIG_DIR"/etc/network/interfaces
fi

mkdir -p "$HBN_CONFIG_DIR"/etc/hbn-users

cp etc/systemd/network/30-hbn-mtu.network /etc/systemd/network/30-hbn-mtu.network
chmod a+r /etc/systemd/network/30-hbn-mtu.network

if [ -n "$mtu" ]; then
	sed -i "s/MTUBytes=9216/MTUBytes=$mtu/g" /etc/systemd/network/30-hbn-mtu.network
fi

echo "HBN setup completed"
