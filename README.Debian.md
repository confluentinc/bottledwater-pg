Bottled Water - Debian Packaging
================================

This is a packaging branch for maintaining some Debian packages for private use.
The primary maintained branch is at
[confluentinc/bottledwater-pg#master](https://github.com/confluentinc/bottledwater-pg).


Building packages from master
-----------------------------

The basic workflow for releasing a new package:

1. Check out the `debian` packaging branch (this branch).

2. Merge in the commits you want: e.g. `REVISION=confluentinc/master make deb-merge`.  This will
   also automatically update `DEBIAN_UPSTREAM_TAG` in the [Makefile](Makefile) and commit it.

3. Mint a new package version and update the changelog: `make deb-release`.  (This will commit a
   change to [debian/changelog](debian/changelog).)

4. Push your commits (merge, Makefile change and changelog change) back to this branch.

5. The push will trigger a [Travis build](https://travis-ci.org/samstokes/bottledwater-pg).  Travis
   will:
    * compile the code
    * build binary packages
    * push the packages to packagecloud.io
    * tag the Git repo to identify the revision the package was built from


Building packages for a branch
------------------------------

If you want to build packages for a branch (e.g. for a feature you want that hasn't yet been merged
upstream), the flow is slightly different:

1. Create a new packaging branch (e.g. `debian-myfeature`) based off the current `debian` branch.
   The packaging branch name should begin with `debian-` in order for Travis to build it.

2. Update `DEBIAN_BRANCH` in the [Makefile](Makefile) to match the name of your packaging branch
   (e.g. `debian-myfeature`).

3. Merge in the branch you want: e.g. `REVISION=myfork/myfeature make deb-merge`.  This will
   also automatically update `DEBIAN_UPSTREAM_TAG` in the [Makefile](Makefile) and commit it.

4. Begin a new version series in the changelog: `VERSION=0.1++myfeature-0ubuntu1 make
   deb-new-release`.  This ensures that the branch name will be reflected in the package version
   string.  (This will commit a change to [debian/changelog](debian/changelog).)

5. Push your packaging branch (merge, Makefile changes and changelog change) to the `samstokes` fork.

6. The push will trigger a [Travis build](https://travis-ci.org/samstokes/bottledwater-pg) as above.

If you need to build subsequent packages from the same branch (e.g. to incorporate further progress
on the branch), the process is similar except using `make deb-release` instead of `VERSION=blah make
deb-new-release`.


Building packages locally
-------------------------

If you want to test the package build locally (e.g. if Travis is broken or you've made changes to
the packaging itself), the easiest way is to use the provided `deb-*-docker` Make targets, which
attempt to provide a clean build environment and replicate the behaviour of the Travis build:

1. Set up a build environment: `ARCH=amd64 DIST=precise make deb-prepare-docker`.  This will take a
   long time, but caches the result in `/tmp/pbuilder-cache` so you'll only need to run it once
   (until your /tmp is cleared).

2. Run the package build: `ARCH=amd64 DIST=precise make deb-build-docker`.
    * N.B. currently this will build the package but then delete the Docker image containing it, so
      if you want to retrieve the .deb you'll need to modify the `docker` command used by this Make
      target (e.g. remove the `--rm`).


Troubleshooting Debian packaging
--------------------------------

Building a Debian package has a surprising number of moving parts, so things can break.

### `make deb-release` fails with "Version x not found"

e.g.

    $ make deb-release
    gbp dch --debian-branch=debian --release --distribution=trusty --commit
    gbp:error: Version 0.1+master-0ubuntu3 not found
    Makefile:28: recipe for target 'deb-release' failed
    make: *** [deb-release] Error 1

This can happen if you haven't synced all the tags from the repo, or if the Travis build failed (or
has not yet run).  The tool that updates the Debian changelog needs to grab all commit messages
since the last build, so it looks for a tag matching the most recent existing changelog entry.

First, see if syncing tags fixes it: `git fetch --tags samstokes`

If that syncs down the tag that was "not found", then try `make deb-release` again and it should
work now.  If not, you may need to troubleshoot the [Travis
build](https://travis-ci.org/samstokes/bottledwater-pg) to find out why the tag was not pushed.

As a workaround, you can manually create the missing tag locally:

    git tag debian/0.1+master-0ubuntu3 $REF
    # $REF should be the git revision you expected the previous package version
    # to have built from.

Please *don't* push this tag to the repo unless you want to confuse people!
