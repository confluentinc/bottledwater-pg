.PHONY: all install clean

all:
	$(MAKE) -C ext all
	$(MAKE) -C client all

install:
	$(MAKE) -C ext install

clean:
	$(MAKE) -C ext clean
	$(MAKE) -C client clean
