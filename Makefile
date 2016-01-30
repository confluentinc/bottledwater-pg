.PHONY: all install clean

DEBIAN_BRANCH = debian

# For git-buildpackage's mysterious purposes we need to tell it the revision
# of the upstream branch we are building from.  (We can't just give it the
# branch name because Travis' git clone doesn't include branch refs.)
# XXX update this if you merge in new changes from upstream!
DEBIAN_UPSTREAM_TAG = 7b864b2

all:
	$(MAKE) -C ext all
	$(MAKE) -C client all
	$(MAKE) -C kafka all

install:
	$(MAKE) -C ext install

clean:
	$(MAKE) -C ext clean
	$(MAKE) -C client clean
	$(MAKE) -C kafka clean

deb-snapshot:
	gbp dch --debian-branch=$(DEBIAN_BRANCH) --snapshot

deb-release:
	gbp dch --debian-branch=$(DEBIAN_BRANCH) --release --distribution=trusty --commit

deb-chroot-vars:
	test -n "${DIST}" || { echo Please set DIST >&2; exit 1; }
	test -n "${ARCH}" || { echo Please set ARCH >&2; exit 1; }

deb-prepare: deb-chroot-vars
	git-pbuilder create --components 'main universe' --hookdir debian/pbuilder-hooks

deb-build: deb-chroot-vars
	sed -i "s:trusty:${DIST}:g" debian/changelog
	gbp buildpackage -us -uc --git-ignore-branch --git-upstream-tag=$(DEBIAN_UPSTREAM_TAG) --git-verbose --git-tag --git-ignore-new --git-pbuilder --git-arch=${ARCH} --git-dist=${DIST}

DOCKER_IMAGE = bwdeb
PBUILDER_CACHE = /tmp/pbuilder-cache

deb-docker:
	docker build -t $(DOCKER_IMAGE) -f build/Dockerfile.debian .

deb-prepare-docker: deb-chroot-vars deb-docker
	docker run --rm --privileged=true -e DIST=${DIST} -e ARCH=${ARCH} -v $(PBUILDER_CACHE):/var/cache/pbuilder $(DOCKER_IMAGE) make deb-prepare

deb-build-docker: deb-chroot-vars deb-docker
# not making deb-prepare-docker a dependency to avoid rebuilding the chroot every time
	docker run --rm --privileged=true -e DIST=${DIST} -e ARCH=${ARCH} -v $(PBUILDER_CACHE):/var/cache/pbuilder $(DOCKER_IMAGE) make deb-build

deb-clean-docker:
	sudo rm -rf $(PBUILDER_CACHE)
