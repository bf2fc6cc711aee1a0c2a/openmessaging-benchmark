We do not yet have a proper maven repo.  The temporary workaround is to publish to https://github.com/shawkins/repo

After checking it out run:

mvn deploy -DcreateChecksum=true -DaltDeploymentRepository=snapshot-repo::default::file:../repo

Then commit the changes.