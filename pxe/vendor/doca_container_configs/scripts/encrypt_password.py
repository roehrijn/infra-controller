import getopt
import sys
import logging
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes

short_options = "hu:p:d:"
long_options = ["help", "username=", "password=", "directory="]
isUsername = False
isPassword = False
directory = "/var/lib/hbn/etc/hbn-users/"

def logConfigure(logLevel):
    logging.basicConfig(filename = '/var/log/doca/hbn/decrypt-user-add.log',
                        level = logLevel,
                        format = '%(asctime)s:%(levelname)s:%(filename)s:%(name)s:%(message)s')

def encryptPassword(password, password_file, public_key):
    try:
        encMessage = public_key.encrypt(
            password,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
    except Exception as e:
        logging.error("Failed to encrypt password for username %s" % (username))
    try:
        with open(password_file, "wb") as binary_file:
            binary_file.write(encMessage)
            binary_file.close()
    except Exception as e:
        logging.error("Failed store encypted data to file for username %s" % (username))

logConfigure(logging.INFO)
full_cmd_arguments = sys.argv
argument_list = full_cmd_arguments[1:]

try:
    arguments, values = getopt.getopt(argument_list, short_options, long_options)
except getopt.error as err:
    print('encrypt_password.py -u|--username <username> -p|--password <password> -d|--directory <directory_path>')
    sys.exit(2)

for current_argument, current_value in arguments:
    if current_argument in ("-h", "--help"):
        print('encrypt_password.py -u|--username <username> -p|--password <password> -d|--directory <directory_path>')
        sys.exit(2)

for current_argument, current_value in arguments:
    if current_argument in ("-u", "--username"):
        username = str(current_value)
        isUsername = True
    elif current_argument in ("-p", "--password"):
        password = str(current_value)
        isPassword = True
    elif current_argument in ("-d", "--directory"):
        directory = str(current_value)

if isUsername == False or isPassword == False:
    print('Please provide bothe username and password')
    sys.exit(2)

public_key_str = b'''-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4ro/k/2k9nl+I+K6pjkw
m0UJL7l4ZWiJYWjnfwniyri8RrjW9h/qyBepbVBoqiuTbocHFjgFVVSp3d2HII7Y
Cfk4VtZYpWdFRo4Jo4DAvpOqteWFe7tgXtbxznWw8j/CNPA4jIPNCYlXZYBDMM4L
5TedxZl7biuSUtmy97+RyokReBpkm6yfIJxQuKIamR5ijZ56JBqLYQGuimOGn92w
fObcv4U6wOimgOpHwhWRqHL/3gwH1JTqEABGUTK9m1wXRRklk9hbrns6/QGPoqvU
zlUPeaP9+J43Ye6x6usCeeDNSrBsZs8G8yzveltVpXjjQYSY3gVg12Bq2yM5C68c
3wIDAQAB
-----END PUBLIC KEY-----'''

try:
    public_key = serialization.load_pem_public_key(public_key_str, backend=default_backend())
except Exception as e:
    logging.error("Failed to load the public key")

password_file = directory + "/" + username + ".pass"
password = password.encode('ascii')

encryptPassword(password, password_file, public_key)
