#!/bin/bash
set -e

if ! command -v aws &> /dev/null
then
  echo "Please install the AWS CLI."
  exit 1
fi

if ! command -v pwgen &> /dev/null
then
  echo "Please install pwgen."
  exit 2
fi

if ! command -v gpg &> /dev/null
then
  echo "Please install gpg."
  exit 3
fi

# you'll also need the AWS CLI to be configured with credentials for our AWS account.

AWS_DEFAULT_REGION=us-west-2
FLUX_REALNAME="Flux SWF Client"
FLUX_EMAIL="dan+flux@danielgmyers.com"

TEMPDIR=$(mktemp -d)
echo "Working in ${TEMPDIR}..."

echo "Generating passphrase..."
FLUX_GPG_PASSPHRASE=$(pwgen -s 16 1)

echo "Generating key..."
gpg --homedir $TEMPDIR --batch --gen-key <<FOO
    Key-Type: RSA
    Key-Length: 4096
    Name-Real: $FLUX_REALNAME
    Name-Email: $FLUX_EMAIL
    Expire-Date: 1y
    Passphrase: $FLUX_GPG_PASSPHRASE
    %commit
    %echo done
FOO

# the output of "gpg --list-keys --with-colons --fingerprint" looks something like this:

# $ gpg --homedir $TEMPDIR --list-public-keys --with-colons --fingerprint
# tru::1:1648579633:0:3:1:5
# pub:-:4096:1:7C5DC8336806E60E:1607727690:1680116664::-:::scESC::::::23::0:
# fpr:::::::::A9C2E0DFFED2658D75A64D7A7C5DC8336806E60E:
# uid:-::::1648580664::5F4E53DF54AD0F951384C0F6051B09F3E2BE8496::AWS Flux SWF Client <aws-flux-swf-client-owners@amazon.com>::::::::::0:
# sub:-:4096:1:D8B3CEEC5C428011:1607727690:1680116665:::::e::::::23:
# fpr:::::::::FB6E627D30C5B1F4C2177B1BD8B3CEEC5C428011:

# we want the hex string from the fingerprint line just before the uid.

echo "Retrieving key id..."
FLUX_GPG_KEY_ID=$(gpg --homedir $TEMPDIR --list-public-keys --with-colons --fingerprint | grep -B 1 $FLUX_EMAIL | head -n 1 | cut -d ':' -f 10)

echo "Exporting public key..."
FLUX_GPG_PUBLIC_KEY=$(gpg --homedir $TEMPDIR --armor --export $FLUX_GPG_KEY_ID)

echo "Exporting private key..."
FLUX_GPG_PRIVATE_KEY=$(gpg --homedir $TEMPDIR --batch --armor --pinentry-mode loopback --passphrase $FLUX_GPG_PASSPHRASE --export-secret-keys $FLUX_GPG_KEY_ID)

# This assumes CreateSecret has already been done for each of these secrets. SecretsManager doesn't have an "upsert" operation.

echo "Saving key id in secrets manager..."
aws secretsmanager put-secret-value --secret-id gpg-signing-key-id --secret-string "$FLUX_GPG_KEY_ID"

echo "Saving passphrase in secrets manager..."
aws secretsmanager put-secret-value --secret-id gpg-signing-key-passphrase --secret-string "$FLUX_GPG_PASSPHRASE"

echo "Saving public key in secrets manager..."
aws secretsmanager put-secret-value --secret-id gpg-signing-key-public-key --secret-string "$FLUX_GPG_PUBLIC_KEY"

echo "Saving private key in secrets manager..."
aws secretsmanager put-secret-value --secret-id gpg-signing-key-private-key --secret-string "$FLUX_GPG_PRIVATE_KEY"

echo "Sending public key to keys.openpgp.org..."
gpg --homedir $TEMPDIR --keyserver hkps://keys.openpgp.org --send-keys $FLUX_GPG_KEY_ID

echo "Cleaning up ${TEMPDIR}..."
rm -rf $TEMPDIR
