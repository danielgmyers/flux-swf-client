#!/bin/bash
set +e

if ! command -v aws &> /dev/null
then
  echo "Please install the AWS CLI."
  exit 1
fi

if ! command -v jq &> /dev/null
then
  echo "Please install jq."
  exit 2
fi

if ! command -v gpg &> /dev/null
then
  echo "Please install gpg."
  exit 3
fi

# you'll also need the AWS CLI to be configured with credentials for our AWS account.

AWS_DEFAULT_REGION=us-west-2

FLUX_GPG_KEY_ID=$(aws secretsmanager get-secret-value --secret-id gpg-signing-key-id | jq -r .SecretString)
FLUX_GPG_PASSPHRASE=$(aws secretsmanager get-secret-value --secret-id gpg-signing-key-passphrase | jq -r .SecretString)
FLUX_GPG_PRIVATE_KEY=$(aws secretsmanager get-secret-value --secret-id gpg-signing-key-private-key | jq -r .SecretString)

TEMPDIR=$(mktemp -d)
echo "Working in ${TEMPDIR}..."

echo "Importing private key"
gpg --homedir $TEMPDIR --batch --passphrase $FLUX_GPG_PASSPHRASE --import <<EOF
$FLUX_GPG_PRIVATE_KEY
EOF

echo "Renewing expiration date..."
gpg --homedir $TEMPDIR --batch --pinentry-mode loopback --passphrase $FLUX_GPG_PASSPHRASE --command-fd 0 --status-fd 2 --edit-key $FLUX_GPG_KEY_ID <<EOF
expire
1y
key 1
expire
1y
save
EOF

echo "Exporting renewed public key..."
FLUX_GPG_PUBLIC_KEY=$(gpg --homedir $TEMPDIR --armor --export $FLUX_GPG_KEY_ID)

echo "Exporting renewed private key..."
FLUX_GPG_PRIVATE_KEY=$(gpg --homedir $TEMPDIR --batch --armor --pinentry-mode loopback --passphrase $FLUX_GPG_PASSPHRASE --export-secret-keys $FLUX_GPG_KEY_ID)

echo "Saving updated public key in secrets manager..."
aws secretsmanager put-secret-value --secret-id gpg-signing-key-public-key --secret-string "$FLUX_GPG_PUBLIC_KEY"

echo "Saving updated private key in secrets manager..."
aws secretsmanager put-secret-value --secret-id gpg-signing-key-private-key --secret-string "$FLUX_GPG_PRIVATE_KEY"

echo "Sending updated public key to keys.openpgp.org..."
gpg --homedir $TEMPDIR --keyserver hkps://keys.openpgp.org --send-keys $FLUX_GPG_KEY_ID

echo "Cleaning up ${TEMPDIR}..."
rm -rf $TEMPDIR
