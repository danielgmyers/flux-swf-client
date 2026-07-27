#!/bin/bash

TO_PUBLISH=()

check_module_published() {
  local module=$1

  if [ ! -f "${module}/pom.xml" ]; then
    echo "Could not find ${module}/pom.xml"
    exit 1
  fi

  local filename="/tmp/${module}-effective-pom.xml"
  # dumps the effective pom for this module, and ignores the log output that isn't the xml
  mvn help:effective-pom -pl :${module} -B | grep "^\s*<" > $filename

  local group_id=$(xmllint --xpath "string(/*[local-name()='project']/*[local-name()='groupId'])" $filename)
  local artifact_id=$(xmllint --xpath "string(/*[local-name()='project']/*[local-name()='artifactId'])" $filename)
  local version=$(xmllint --xpath "string(/*[local-name()='project']/*[local-name()='version'])" $filename)

  #echo "Checking ${group_id//./\/}/${artifact_id}/${version}"

  local md_url="https://repo1.maven.org/maven2/${group_id//./\/}/${artifact_id}/maven-metadata.xml"
  local md_file="/tmp/${module}-md.xml"

  wget -q $md_url -O /tmp/${module}-md.xml
  if [ $? -ne 0 ] ; then
    #echo "$artifact_id appears to be a new package, including $version for publishing."
    TO_PUBLISH+=(":${module}")
  elif ! xmllint --xpath "/metadata/versioning/versions/version[text()='${version}']" $md_file > /dev/null; then
    TO_PUBLISH+=(":${module}")
    #echo "Including $version for publishing."
  #else
    #echo "$version is already published."
  fi

  rm $md_file
  rm $filename
}

check_module_published 'flux-common'
check_module_published 'flux-common-aws'
check_module_published 'flux-testutils'

check_module_published 'flux-swf'
check_module_published 'flux-swf-guice'
check_module_published 'flux-swf-spring'

#sfn not ready for release yet
#check_module_published 'flux-sfn'
#check_module_published 'flux-sfn-guice'
#check_module_published 'flux-sfn-spring'

if [ "${#TO_PUBLISH[@]}" == 0 ]; then
    echo "Nothing to publish"
    exit 1
fi

echo $(IFS=, ; echo "${TO_PUBLISH[*]}")
exit 0
