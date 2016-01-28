.PHONY: all install clean

DEBIAN_BRANCH = debian

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
