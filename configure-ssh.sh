#!/bin/sh
# Usage: ./configure-ssh.sh myusername myhost mypassword
sshpass -p $3 ssh $1@$2 "mkdir -p .ssh ; chmod 700 .ssh ; chown -R $1:$1 .ssh ; touch .ssh/authorized_keys ; chmod 600 .ssh/authorized_keys"
cat ~/.ssh/id_rsa.pub | sshpass -p $3 ssh $1@$2 "cat - >> .ssh/authorized_keys"
ssh $1@$2 hostname

